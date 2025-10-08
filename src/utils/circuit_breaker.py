"""
Circuit breaker pattern implementation for API calls.
Prevents cascading failures when external services are unavailable.
"""
import time
import logging
from typing import Optional, Callable, Any, TypeVar, Generic, Type
from functools import wraps
from dataclasses import dataclass
from enum import Enum, auto

logger = logging.getLogger(__name__)

T = TypeVar('T')

class CircuitState(Enum):
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()

@dataclass
class CircuitBreakerStats:
    failures: int = 0
    successes: int = 0
    last_failure: float = 0
    last_success: float = 0

class CircuitBreakerError(Exception):
    """Raised when the circuit is open and the call is not allowed."""
    pass

class CircuitBreaker(Generic[T]):
    """
    Implements the circuit breaker pattern for API calls.
    
    Args:
        failure_threshold: Number of failures before opening the circuit
        recovery_timeout: Time in seconds before trying to close the circuit
        expected_exceptions: Tuple of exceptions that should be considered failures
        max_failures: Maximum number of failures before opening the circuit
        name: Name of the circuit breaker for logging
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exceptions: tuple[Type[Exception], ...] = (Exception,),
        max_failures: int = 10,
        name: str = "default"
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exceptions = expected_exceptions
        self.max_failures = max_failures
        self.name = name
        self.state = CircuitState.CLOSED
        self.stats = CircuitBreakerStats()
        self._state_transition_time = time.time()
    
    def __call__(self, func: Callable[..., T]) -> Callable[..., T]:
        """Decorator to wrap a function with circuit breaker logic."""
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            return self.call(func, *args, **kwargs)
        return wrapper
    
    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """
        Execute the function with circuit breaker protection.
        
        Args:
            func: The function to call
            *args: Positional arguments to pass to the function
            **kwargs: Keyword arguments to pass to the function
            
        Returns:
            The result of the function call
            
        Raises:
            CircuitBreakerError: If the circuit is open
            Exception: Any exception raised by the wrapped function
        """
        if self.state == CircuitState.OPEN:
            # Check if we should try to close the circuit
            if time.time() - self._state_transition_time > self.recovery_timeout:
                logger.info(f"Circuit breaker {self.name} moving to HALF-OPEN state")
                self.state = CircuitState.HALF_OPEN
                self.stats = CircuitBreakerStats()
            else:
                logger.warning(f"Circuit breaker {self.name} is OPEN, request blocked")
                raise CircuitBreakerError(f"Circuit {self.name} is OPEN")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
            
        except self.expected_exceptions as e:
            self._on_failure()
            logger.error(f"Circuit breaker {self.name} caught exception: {e}")
            raise
    
    def _on_success(self):
        """Handle a successful call."""
        self.stats.successes += 1
        self.stats.last_success = time.time()
        
        if self.state == CircuitState.HALF_OPEN:
            # If we've had enough successes, close the circuit
            if self.stats.successes >= self.failure_threshold:
                self._close()
    
    def _on_failure(self):
        """Handle a failed call."""
        self.stats.failures += 1
        self.stats.last_failure = time.time()
        
        if self.state == CircuitState.HALF_OPEN or \
           (self.state == CircuitState.CLOSED and 
            self.stats.failures >= self.failure_threshold):
            self._open()
    
    def _open(self):
        """Open the circuit."""
        old_state = self.state
        self.state = CircuitState.OPEN
        self._state_transition_time = time.time()
        
        if old_state != CircuitState.OPEN:
            logger.warning(f"Circuit breaker {self.name} is now OPEN")
    
    def _close(self):
        """Close the circuit."""
        old_state = self.state
        self.state = CircuitState.CLOSED
        self._state_transition_time = time.time()
        self.stats = CircuitBreakerStats()  # Reset stats
        
        if old_state != CircuitState.CLOSED:
            logger.info(f"Circuit breaker {self.name} is now CLOSED")
    
    def get_state(self) -> CircuitState:
        """Get the current state of the circuit breaker."""
        return self.state
    
    def get_stats(self) -> CircuitBreakerStats:
        """Get the current statistics."""
        return self.stats

# Global circuit breakers
binance_api_breaker = CircuitBreaker(
    name="binance_api",
    failure_threshold=5,
    recovery_timeout=60,
    expected_exceptions=(Exception,),
    max_failures=10
)

database_breaker = CircuitBreaker(
    name="database",
    failure_threshold=3,
    recovery_timeout=30,
    expected_exceptions=(Exception,),
    max_failures=5
)
