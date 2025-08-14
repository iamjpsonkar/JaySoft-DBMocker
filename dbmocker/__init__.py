"""
JaySoft-DBMocker - A comprehensive tool for generating realistic mock data for SQL databases.

This package provides tools to:
- Analyze database schemas and existing data
- Generate realistic mock data respecting all constraints
- Handle large-scale data generation efficiently
- Support multiple database engines (PostgreSQL, MySQL, SQLite)
"""

__version__ = "1.0.0"
__author__ = "JaySoft Development"
__email__ = "info@jaysoft.dev"

from dbmocker.core.database import DatabaseConnection
from dbmocker.core.analyzer import SchemaAnalyzer
from dbmocker.core.generator import DataGenerator
from dbmocker.core.inserter import DataInserter

__all__ = [
    "DatabaseConnection",
    "SchemaAnalyzer", 
    "DataGenerator",
    "DataInserter",
]
