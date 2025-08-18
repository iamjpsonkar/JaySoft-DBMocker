"""Database connection and management utilities."""

import logging
from typing import Dict, Any, Optional
from sqlalchemy import create_engine, Engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel, Field, validator


logger = logging.getLogger(__name__)


class DatabaseConfig(BaseModel):
    """Configuration model for database connections."""
    
    driver: str = Field(default="postgresql", description="Database driver")
    host: str = Field(..., description="Database host")
    port: int = Field(..., description="Database port")
    database: str = Field(default="", description="Database name")
    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")
    ssl_mode: Optional[str] = Field(default=None, description="SSL mode")
    charset: str = Field(default="utf8mb4", description="Character set")
    
    @validator("driver")
    def validate_driver(cls, v):
        supported_drivers = ["postgresql", "mysql", "sqlite"]
        if v not in supported_drivers:
            raise ValueError(f"Unsupported driver: {v}. Supported: {supported_drivers}")
        return v
    
    @validator("port")
    def validate_port(cls, v, values):
        # Allow port 0 for SQLite (SQLite doesn't use ports)
        driver = values.get("driver", "postgresql")  # Default to postgresql if not set
        if driver == "sqlite":
            return v  # Any port value is acceptable for SQLite, will be ignored
        if not 1 <= v <= 65535:
            raise ValueError("Port must be between 1 and 65535")
        return v


class DatabaseConnection:
    """Manages database connections and provides utilities for database operations."""
    
    def __init__(self, config: DatabaseConfig):
        """Initialize database connection with configuration."""
        self.config = config
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None
        
    def connect(self) -> None:
        """Establish connection to the database."""
        try:
            connection_url = self._build_connection_url()
            logger.info(f"Connecting to {self.config.driver} database at {self.config.host}:{self.config.port}")
            
            # Configure engine with proper transaction isolation
            engine_kwargs = {
                "echo": False,
                "pool_pre_ping": True,
                "pool_recycle": 3600,
                "connect_args": self._get_connect_args()
            }
            
            # Add autocommit configuration for better transaction control
            if self.config.driver != "sqlite":
                engine_kwargs["isolation_level"] = "AUTOCOMMIT"
            
            self._engine = create_engine(connection_url, **engine_kwargs)
            
            # Test connection
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                logger.info("Database connection established successfully")
            
            self._session_factory = sessionmaker(bind=self._engine)
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to connect to database: {e}")
            raise ConnectionError(f"Database connection failed: {e}")
    
    def _build_connection_url(self) -> str:
        """Build SQLAlchemy connection URL from config."""
        if self.config.driver == "postgresql":
            driver_name = "postgresql+psycopg2"
        elif self.config.driver == "mysql":
            driver_name = "mysql+pymysql"
        elif self.config.driver == "sqlite":
            return f"sqlite:///{self.config.database}"
        else:
            raise ValueError(f"Unsupported driver: {self.config.driver}")
        
        # Build base URL
        base_url = f"{driver_name}://{self.config.username}:{self.config.password}@{self.config.host}:{self.config.port}"
        
        # Add database name if specified (for database-specific connections)
        if self.config.database:
            return f"{base_url}/{self.config.database}"
        else:
            # Server-level connection (for listing databases)
            return base_url
    
    def _get_connect_args(self) -> Dict[str, Any]:
        """Get driver-specific connection arguments."""
        args = {}
        
        if self.config.driver == "mysql":
            args["charset"] = self.config.charset
            if self.config.ssl_mode:
                args["ssl_mode"] = self.config.ssl_mode
        elif self.config.driver == "postgresql":
            if self.config.ssl_mode:
                args["sslmode"] = self.config.ssl_mode
        
        return args
    
    @property
    def engine(self) -> Engine:
        """Get the SQLAlchemy engine."""
        if self._engine is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._engine
    
    def get_session(self) -> Session:
        """Get a new database session."""
        if self._session_factory is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._session_factory()
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a raw SQL query and return results."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return result.fetchall()
        except SQLAlchemyError as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test if the database connection is alive."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError:
            return False
    
    def close(self) -> None:
        """Close database connection and cleanup resources."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self._session_factory = None
            logger.info("Database connection closed")
    
    def quote_identifier(self, identifier: str) -> str:
        """Quote table or column name properly based on database type."""
        if self.config.driver == "mysql":
            return f"`{identifier}`"
        elif self.config.driver == "postgresql":
            return f'"{identifier}"'
        elif self.config.driver == "sqlite":
            return f'"{identifier}"'
        else:
            return identifier
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def create_database_connection(
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    driver: str = "postgresql",
    **kwargs
) -> DatabaseConnection:
    """Factory function to create a database connection."""
    config = DatabaseConfig(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        driver=driver,
        **kwargs
    )
    return DatabaseConnection(config)
