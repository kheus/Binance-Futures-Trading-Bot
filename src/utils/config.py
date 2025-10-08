"""
Configuration management for the trading bot.
Loads and validates configuration from YAML files and environment variables.
"""
import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, TypeVar, Type
from pydantic import BaseModel, validator, Field
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

# Load environment variables from .env file if it exists
load_dotenv()

T = TypeVar('T', bound='BaseConfig')

class BaseConfig(BaseModel):
    """Base configuration class with common functionality."""
    
    class Config:
        extra = 'forbid'  # Forbid extra fields
        validate_assignment = True
        
    @classmethod
    def from_yaml(cls: Type[T], file_path: Path) -> T:
        """Load configuration from a YAML file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f) or {}
            return cls(**config_data)
        except Exception as e:
            logger.error(f"Failed to load config from {file_path}: {e}")
            raise
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return self.dict(exclude_unset=True)

class DatabaseConfig(BaseConfig):
    """Database configuration."""
    host: str = Field(default="localhost", env="DB_HOST")
    port: int = Field(default=5432, env="DB_PORT")
    database: str = Field(default="trading_bot", env="DB_NAME")
    user: str = Field(default="postgres", env="DB_USER")
    password: str = Field(default="", env="DB_PASSWORD")
    min_connections: int = 2
    max_connections: int = 20
    connect_timeout: int = 10
    
    @validator('password', pre=True)
    def validate_password(cls, v):
        if not v and not os.getenv('DB_PASSWORD'):
            logger.warning("Database password is not set")
        return v or os.getenv('DB_PASSWORD', '')

class BinanceConfig(BaseConfig):
    """Binance API configuration."""
    api_key: str = Field(..., env="BINANCE_API_KEY")
    api_secret: str = Field(..., env="BINANCE_API_SECRET")
    base_url: str = "https://fapi.binance.com"
    testnet: bool = False
    
    @validator('api_key', 'api_secret', pre=True)
    def validate_credentials(cls, v, field):
        env_var = f"BINANCE_{field.name.upper()}"
        value = os.getenv(env_var, v)
        if not value:
            raise ValueError(f"{env_var} is required")
        return value
    
    @property
    def ws_url(self) -> str:
        """Get WebSocket URL based on testnet setting."""
        if self.testnet:
            return "wss://testnet.binance.vision/ws"
        return "wss://fstream.binance.com/ws"

class TradingConfig(BaseConfig):
    """Trading configuration."""
    symbols: list[str] = ["BTCUSDT"]
    timeframe: str = "1h"
    capital: float = 1000.0
    leverage: int = 10
    max_risk_per_trade: float = 0.02
    max_drawdown: float = 0.1
    trailing_stop: bool = True
    
    @validator('leverage')
    def validate_leverage(cls, v):
        if not 1 <= v <= 100:
            raise ValueError("Leverage must be between 1 and 100")
        return v
    
    @validator('max_risk_per_trade')
    def validate_risk(cls, v):
        if not 0 < v <= 1:
            raise ValueError("Risk per trade must be between 0 and 1")
        return v

class LoggingConfig(BaseConfig):
    """Logging configuration."""
    level: str = "INFO"
    file: Optional[str] = "trading_bot.log"
    max_size: int = 10  # MB
    backup_count: int = 5
    
    @validator('level')
    def validate_level(cls, v):
        levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in levels:
            raise ValueError(f"Invalid log level. Must be one of {levels}")
        return v.upper()

class Config(BaseConfig):
    """Main configuration class."""
    database: DatabaseConfig = DatabaseConfig()
    binance: BinanceConfig
    trading: TradingConfig = TradingConfig()
    logging: LoggingConfig = LoggingConfig()
    
    @classmethod
    def load(cls, config_path: Optional[str] = None) -> 'Config':
        """Load configuration from file and environment."""
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config" / "config.yaml"
        
        if not config_path.exists():
            logger.warning(f"Config file not found at {config_path}, using defaults")
            return cls()
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f) or {}
            
            # Handle nested configurations
            database_config = config_data.pop('database', {})
            binance_config = config_data.pop('binance', {})
            trading_config = config_data.pop('trading', {})
            logging_config = config_data.pop('logging', {})
            
            return cls(
                database=DatabaseConfig(**database_config),
                binance=BinanceConfig(**binance_config),
                trading=TradingConfig(**trading_config),
                logging=LoggingConfig(**logging_config),
                **config_data
            )
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            raise

# Global config instance
config = Config.load()
