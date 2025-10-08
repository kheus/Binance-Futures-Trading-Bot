"""
Retry utilities for handling transient failures.
Provides decorators and context managers for retrying operations.
"""
import time
import random
import logging
from typing import Callable, Type, Tuple, Optional, Any, TypeVar, cast
from functools import wraps

from tenacity import (
    retry,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential,
    wait_random,
    retry_if_exception_type,
    retry_if_result,
    RetryCallState,
    Retrying,
    before_sleep_log,
    retry_any,
    retry_all,
    retry_if_exception,
    wait_chain,
    wait_fixed,
    wait_random_exponential,
)

from .circuit_breaker import CircuitBreakerError

logger = logging.getLogger(__name__)

T = TypeVar('T')
FunctionT = TypeVar('FunctionT', bound=Callable[..., Any])

class MaxRetriesExceededError(Exception):
    """Raised when the maximum number of retries is exceeded."""
    def __init__(self, message: str, last_exception: Optional[Exception] = None):
        super().__init__(message)
        self.last_exception = last_exception

def is_retryable_exception(e: Exception) -> bool:
    """Determine if an exception is retryable."""
    retryable_exceptions = (
        ConnectionError,
        TimeoutError,
        OSError,
        CircuitBreakerError,
    )
    
    # Check for common retryable error messages
    retryable_messages = [
        "too many requests",
        "rate limit",
        "timeout",
        "connection",
        "network",
        "temporary",
        "busy",
        "try again",
        "service unavailable",
        "gateway",
    ]
    
    # Check if exception is in retryable types
    if isinstance(e, retryable_exceptions):
        return True
    
    # Check error message for retryable patterns
    error_msg = str(e).lower()
    return any(msg in error_msg for msg in retryable_messages)

def retry_with_backoff(
    func: Optional[Callable[..., T]] = None,
    *,
    max_retries: int = 3,
    max_delay: float = 30.0,
    initial_delay: float = 1.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retry_on: Tuple[Type[Exception], ...] = (Exception,),
    retry_on_result: Optional[Callable[[T], bool]] = None,
    on_retry: Optional[Callable[[int, float, Exception], None]] = None,
) -> Callable[..., T]:
    """
    Retry a function with exponential backoff.
    
    Args:
        func: The function to retry
        max_retries: Maximum number of retry attempts
        max_delay: Maximum delay between retries in seconds
        initial_delay: Initial delay in seconds
        exponential_base: Base for exponential backoff
        jitter: Whether to add random jitter to delays
        retry_on: Tuple of exceptions to retry on
        retry_on_result: Function that takes the result and returns True to retry
        on_retry: Callback function called before each retry
        
    Returns:
        The result of the function call
    """
    def decorator(f: Callable[..., T]) -> Callable[..., T]:
        @wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            retries = 0
            last_exception = None
            
            while retries <= max_retries:
                try:
                    result = f(*args, **kwargs)
                    
                    # Check if we should retry based on the result
                    if retry_on_result and retry_on_result(result):
                        raise RetryableResultError("Result indicates retry is needed")
                        
                    return result
                    
                except retry_on as e:
                    last_exception = e
                    
                    # Only retry retryable exceptions
                    if not is_retryable_exception(e):
                        raise
                    
                    if retries >= max_retries:
                        break
                    
                    # Calculate delay with exponential backoff and jitter
                    delay = min(
                        initial_delay * (exponential_base ** retries),
                        max_delay
                    )
                    
                    if jitter:
                        delay = delay * (0.5 + random.random() * 0.5)  # 0.5-1.5 * delay
                    
                    # Call on_retry callback if provided
                    if on_retry:
                        on_retry(retries, delay, e)
                    
                    logger.warning(
                        f"Retry {retries + 1}/{max_retries} for {f.__name__} after "
                        f"{delay:.2f}s: {str(e)}"
                    )
                    
                    time.sleep(delay)
                    retries += 1
            
            # If we get here, all retries have been exhausted
            raise MaxRetriesExceededError(
                f"Max retries ({max_retries}) exceeded for {f.__name__}",
                last_exception
            )
        
        return wrapper
    
    # Handle both @retry_with_backoff and @retry_with_backoff() syntax
    if func is None:
        return decorator
    return decorator(func)

# Common retry configurations
retry_api = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception(is_retryable_exception),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True
)

retry_database = retry(
    stop=stop_after_attempt(5),
    wait=wait_chain(*[
        wait_fixed(0.1) * 3,  # First 3 retries with 0.1s delay
        wait_exponential(multiplier=1, min=0.5, max=5),  # Then exponential backoff
    ]),
    retry=retry_any(
        retry_if_exception_type(
            (ConnectionError, TimeoutError, OSError)
        ),
        retry_if_exception(
            lambda e: "deadlock" in str(e).lower() or "timeout" in str(e).lower()
        )
    ),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True
)

class RetryableResultError(Exception):
    """Raised when a result indicates a retry is needed."""
    pass

def retry_on_none(max_retries: int = 3) -> Callable[[FunctionT], FunctionT]:
    """Retry a function if it returns None."""
    def decorator(func: FunctionT) -> FunctionT:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            for attempt in range(max_retries):
                result = func(*args, **kwargs)
                if result is not None:
                    return result
                if attempt < max_retries - 1:
                    time.sleep(1 * (attempt + 1))  # Linear backoff
            return None
        return cast(FunctionT, wrapper)
    return decorator

def retry_with_circuit_breaker(
    circuit_breaker,
    max_retries: int = 3,
    **retry_kwargs
):
    """
    Decorator that combines retry logic with circuit breaker.
    
    Args:
        circuit_breaker: CircuitBreaker instance
        max_retries: Maximum number of retries
        **retry_kwargs: Additional arguments for retry_with_backoff
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            def _wrapped():
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if is_retryable_exception(e):
                        circuit_breaker.record_failure()
                    raise
            
            return retry_with_backoff(
                _wrapped,
                max_retries=max_retries,
                **retry_kwargs
            )
        return wrapper
    return decorator
