"""Tests for database connection functionality."""

import pytest
from unittest.mock import Mock, patch
from sqlalchemy.exc import SQLAlchemyError

from dbmocker.core.database import DatabaseConnection, DatabaseConfig, create_database_connection


class TestDatabaseConfig:
    """Test DatabaseConfig model."""
    
    def test_valid_config(self):
        """Test valid database configuration."""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            database="test_db",
            username="test_user",
            password="test_pass",
            driver="postgresql"
        )
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.driver == "postgresql"
    
    def test_invalid_driver(self):
        """Test invalid database driver."""
        with pytest.raises(ValueError, match="Unsupported driver"):
            DatabaseConfig(
                host="localhost",
                port=5432,
                database="test_db",
                username="test_user",
                password="test_pass",
                driver="invalid_driver"
            )
    
    def test_invalid_port(self):
        """Test invalid port number."""
        with pytest.raises(ValueError, match="Port must be between"):
            DatabaseConfig(
                host="localhost",
                port=99999,
                database="test_db",
                username="test_user",
                password="test_pass",
                driver="postgresql"
            )


class TestDatabaseConnection:
    """Test DatabaseConnection class."""
    
    def test_connection_url_postgresql(self):
        """Test PostgreSQL connection URL building."""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            database="test_db",
            username="test_user",
            password="test_pass",
            driver="postgresql"
        )
        db_conn = DatabaseConnection(config)
        url = db_conn._build_connection_url()
        
        expected = "postgresql+psycopg2://test_user:test_pass@localhost:5432/test_db"
        assert url == expected
    
    def test_connection_url_mysql(self):
        """Test MySQL connection URL building."""
        config = DatabaseConfig(
            host="localhost",
            port=3306,
            database="test_db",
            username="test_user",
            password="test_pass",
            driver="mysql"
        )
        db_conn = DatabaseConnection(config)
        url = db_conn._build_connection_url()
        
        expected = "mysql+pymysql://test_user:test_pass@localhost:3306/test_db"
        assert url == expected
    
    def test_connection_url_sqlite(self):
        """Test SQLite connection URL building."""
        config = DatabaseConfig(
            host="localhost",  # Not used for SQLite
            port=5432,         # Not used for SQLite
            database="/path/to/test.db",
            username="test_user",  # Not used for SQLite
            password="test_pass",  # Not used for SQLite
            driver="sqlite"
        )
        db_conn = DatabaseConnection(config)
        url = db_conn._build_connection_url()
        
        expected = "sqlite:///path/to/test.db"
        assert url == expected
    
    @patch('dbmocker.core.database.create_engine')
    def test_connect_success(self, mock_create_engine):
        """Test successful database connection."""
        mock_engine = Mock()
        mock_connection = Mock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            database="test_db",
            username="test_user",
            password="test_pass",
            driver="postgresql"
        )
        
        db_conn = DatabaseConnection(config)
        db_conn.connect()
        
        assert db_conn._engine is not None
        mock_create_engine.assert_called_once()
        mock_connection.execute.assert_called_once()
    
    @patch('dbmocker.core.database.create_engine')
    def test_connect_failure(self, mock_create_engine):
        """Test database connection failure."""
        mock_create_engine.side_effect = SQLAlchemyError("Connection failed")
        
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            database="test_db",
            username="test_user",
            password="test_pass",
            driver="postgresql"
        )
        
        db_conn = DatabaseConnection(config)
        
        with pytest.raises(ConnectionError, match="Database connection failed"):
            db_conn.connect()
    
    def test_engine_not_connected(self):
        """Test accessing engine when not connected."""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            database="test_db",
            username="test_user",
            password="test_pass",
            driver="postgresql"
        )
        
        db_conn = DatabaseConnection(config)
        
        with pytest.raises(RuntimeError, match="Database not connected"):
            _ = db_conn.engine
    
    def test_session_not_connected(self):
        """Test getting session when not connected."""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            database="test_db",
            username="test_user",
            password="test_pass",
            driver="postgresql"
        )
        
        db_conn = DatabaseConnection(config)
        
        with pytest.raises(RuntimeError, match="Database not connected"):
            db_conn.get_session()
    
    @patch('dbmocker.core.database.create_engine')
    def test_context_manager(self, mock_create_engine):
        """Test database connection as context manager."""
        mock_engine = Mock()
        mock_connection = Mock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            database="test_db",
            username="test_user",
            password="test_pass",
            driver="postgresql"
        )
        
        with DatabaseConnection(config) as db_conn:
            assert db_conn._engine is not None
        
        # Engine should be disposed after context exit
        mock_engine.dispose.assert_called_once()


def test_create_database_connection():
    """Test factory function for creating database connection."""
    db_conn = create_database_connection(
        host="localhost",
        port=5432,
        database="test_db",
        username="test_user",
        password="test_pass",
        driver="postgresql"
    )
    
    assert isinstance(db_conn, DatabaseConnection)
    assert db_conn.config.host == "localhost"
    assert db_conn.config.port == 5432
    assert db_conn.config.driver == "postgresql"
