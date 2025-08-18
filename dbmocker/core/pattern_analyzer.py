"""Advanced data pattern analysis for realistic data generation."""

import logging
import re
import json
from typing import Dict, List, Any, Optional, Set, Tuple, Union
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
import statistics
from dataclasses import dataclass, field
from sqlalchemy import text

from .database import DatabaseConnection
from .models import TableInfo, ColumnInfo, ColumnType

logger = logging.getLogger(__name__)


@dataclass
class ColumnPattern:
    """Represents analyzed patterns for a column."""
    column_name: str
    data_type: str
    
    # Basic statistics
    total_records: int = 0
    null_count: int = 0
    unique_count: int = 0
    
    # Value patterns
    most_common_values: List[Tuple[Any, int]] = field(default_factory=list)
    value_distribution: Dict[str, int] = field(default_factory=dict)
    
    # String patterns
    common_prefixes: List[str] = field(default_factory=list)
    common_suffixes: List[str] = field(default_factory=list)
    length_distribution: Dict[int, int] = field(default_factory=dict)
    regex_patterns: List[str] = field(default_factory=list)
    
    # Numeric patterns
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    avg_value: Optional[float] = None
    median_value: Optional[float] = None
    std_dev: Optional[float] = None
    
    # Date patterns
    date_range: Optional[Tuple[datetime, datetime]] = None
    common_day_of_week: Optional[int] = None
    common_hour: Optional[int] = None
    
    # JSON patterns (for JSON columns)
    json_schemas: Dict[str, Any] = field(default_factory=dict)
    json_key_patterns: Set[str] = field(default_factory=set)
    
    # Email patterns
    email_domains: List[str] = field(default_factory=list)
    
    # URL patterns
    url_domains: List[str] = field(default_factory=list)
    url_schemes: List[str] = field(default_factory=list)
    
    # Phone patterns
    phone_formats: List[str] = field(default_factory=list)


@dataclass
class TablePattern:
    """Represents analyzed patterns for a table."""
    table_name: str
    total_records: int
    column_patterns: Dict[str, ColumnPattern] = field(default_factory=dict)
    
    # Relationship patterns
    foreign_key_distributions: Dict[str, Dict[Any, int]] = field(default_factory=dict)
    
    # Cross-column correlations
    correlations: Dict[Tuple[str, str], float] = field(default_factory=dict)


class ExistingDataAnalyzer:
    """Analyzes existing data patterns in database tables."""
    
    def __init__(self, db_connection: DatabaseConnection):
        """Initialize the pattern analyzer."""
        self.db_connection = db_connection
        self.sample_size = 1000  # Default sample size for analysis
        
    def analyze_table_patterns(self, table: TableInfo, sample_size: Optional[int] = None) -> TablePattern:
        """Analyze patterns in existing data for a table."""
        sample_size = sample_size or self.sample_size
        
        logger.info(f"Analyzing data patterns for table: {table.name}")
        
        # Get sample data from table
        sample_data = self._get_sample_data(table.name, sample_size)
        
        if not sample_data:
            logger.warning(f"No data found in table {table.name} for pattern analysis")
            return TablePattern(table_name=table.name, total_records=0)
        
        table_pattern = TablePattern(
            table_name=table.name,
            total_records=len(sample_data)
        )
        
        # Analyze each column
        for column in table.columns:
            column_values = [row.get(column.name) for row in sample_data]
            column_pattern = self._analyze_column_patterns(column, column_values)
            table_pattern.column_patterns[column.name] = column_pattern
        
        # Analyze foreign key distributions
        table_pattern.foreign_key_distributions = self._analyze_foreign_key_patterns(table, sample_data)
        
        # Analyze cross-column correlations
        table_pattern.correlations = self._analyze_correlations(table, sample_data)
        
        logger.info(f"Completed pattern analysis for {table.name}: {len(sample_data)} records analyzed")
        return table_pattern
    
    def _get_sample_data(self, table_name: str, sample_size: int) -> List[Dict[str, Any]]:
        """Get sample data from a table."""
        try:
            quoted_table = self.db_connection.quote_identifier(table_name)
            
            # Get total count first
            count_query = f"SELECT COUNT(*) FROM {quoted_table}"
            count_result = self.db_connection.execute_query(count_query)
            total_records = count_result[0][0] if count_result else 0
            
            if total_records == 0:
                return []
            
            # Determine sampling strategy
            if total_records <= sample_size:
                # Get all records
                query = f"SELECT * FROM {quoted_table}"
            else:
                # Sample records
                if self.db_connection.config.driver == "mysql":
                    query = f"SELECT * FROM {quoted_table} ORDER BY RAND() LIMIT {sample_size}"
                elif self.db_connection.config.driver == "postgresql":
                    query = f"SELECT * FROM {quoted_table} ORDER BY RANDOM() LIMIT {sample_size}"
                else:  # SQLite
                    query = f"SELECT * FROM {quoted_table} ORDER BY RANDOM() LIMIT {sample_size}"
            
            result = self.db_connection.execute_query(query)
            
            if not result:
                return []
            
            # Convert to list of dictionaries
            if result:
                # Get column names from first query execution
                with self.db_connection.get_session() as session:
                    sample_result = session.execute(text(f"SELECT * FROM {quoted_table} LIMIT 1"))
                    columns = list(sample_result.keys())
                return [dict(zip(columns, row)) for row in result]
            else:
                return []
            
        except Exception as e:
            logger.error(f"Failed to get sample data from {table_name}: {e}")
            return []
    
    def _analyze_column_patterns(self, column: ColumnInfo, values: List[Any]) -> ColumnPattern:
        """Analyze patterns in a single column."""
        pattern = ColumnPattern(
            column_name=column.name,
            data_type=column.data_type,
            total_records=len(values)
        )
        
        # Filter out None values for most analysis
        non_null_values = [v for v in values if v is not None]
        pattern.null_count = len(values) - len(non_null_values)
        pattern.unique_count = len(set(non_null_values))
        
        if not non_null_values:
            return pattern
        
        # Most common values
        value_counts = Counter(non_null_values)
        pattern.most_common_values = value_counts.most_common(10)
        
        # Type-specific analysis
        if column.data_type.lower() in ['varchar', 'char', 'text', 'string']:
            self._analyze_string_patterns(pattern, non_null_values)
        elif column.data_type.lower() in ['int', 'integer', 'bigint', 'smallint', 'tinyint', 'decimal', 'float', 'double', 'numeric']:
            self._analyze_numeric_patterns(pattern, non_null_values)
        elif column.data_type.lower() in ['datetime', 'timestamp', 'date', 'time']:
            self._analyze_date_patterns(pattern, non_null_values)
        elif column.data_type.lower() in ['json', 'jsonb']:
            self._analyze_json_patterns(pattern, non_null_values)
        
        # Special column name patterns
        self._analyze_special_column_patterns(pattern, non_null_values, column.name)
        
        return pattern
    
    def _analyze_string_patterns(self, pattern: ColumnPattern, values: List[str]) -> None:
        """Analyze string-specific patterns."""
        str_values = [str(v) for v in values if v is not None]
        
        # Length distribution
        lengths = [len(s) for s in str_values]
        pattern.length_distribution = dict(Counter(lengths))
        
        # Common prefixes and suffixes
        if str_values:
            # Prefixes (first 3 characters)
            prefixes = [s[:3] for s in str_values if len(s) >= 3]
            pattern.common_prefixes = [prefix for prefix, count in Counter(prefixes).most_common(5)]
            
            # Suffixes (last 3 characters)
            suffixes = [s[-3:] for s in str_values if len(s) >= 3]
            pattern.common_suffixes = [suffix for suffix, count in Counter(suffixes).most_common(5)]
        
        # Detect common patterns
        pattern.regex_patterns = self._detect_regex_patterns(str_values)
    
    def _analyze_numeric_patterns(self, pattern: ColumnPattern, values: List[Union[int, float, Decimal]]) -> None:
        """Analyze numeric-specific patterns."""
        numeric_values = []
        for v in values:
            try:
                if isinstance(v, (int, float)):
                    numeric_values.append(float(v))
                elif isinstance(v, Decimal):
                    numeric_values.append(float(v))
                else:
                    numeric_values.append(float(str(v)))
            except (ValueError, TypeError):
                continue
        
        if numeric_values:
            pattern.min_value = min(numeric_values)
            pattern.max_value = max(numeric_values)
            pattern.avg_value = statistics.mean(numeric_values)
            pattern.median_value = statistics.median(numeric_values)
            if len(numeric_values) > 1:
                pattern.std_dev = statistics.stdev(numeric_values)
    
    def _analyze_date_patterns(self, pattern: ColumnPattern, values: List[datetime]) -> None:
        """Analyze date-specific patterns."""
        date_values = []
        for v in values:
            if isinstance(v, datetime):
                date_values.append(v)
            elif isinstance(v, str):
                try:
                    # Try common date formats
                    for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']:
                        try:
                            date_values.append(datetime.strptime(v, fmt))
                            break
                        except ValueError:
                            continue
                except:
                    continue
        
        if date_values:
            pattern.date_range = (min(date_values), max(date_values))
            
            # Common day of week (0=Monday, 6=Sunday)
            days_of_week = [d.weekday() for d in date_values]
            pattern.common_day_of_week = Counter(days_of_week).most_common(1)[0][0]
            
            # Common hour
            hours = [d.hour for d in date_values]
            pattern.common_hour = Counter(hours).most_common(1)[0][0]
    
    def _analyze_json_patterns(self, pattern: ColumnPattern, values: List[str]) -> None:
        """Analyze JSON-specific patterns."""
        json_objects = []
        for v in values:
            try:
                if isinstance(v, str):
                    json_objects.append(json.loads(v))
                elif isinstance(v, dict):
                    json_objects.append(v)
            except (json.JSONDecodeError, TypeError):
                continue
        
        if json_objects:
            # Collect all keys used
            all_keys = set()
            for obj in json_objects:
                if isinstance(obj, dict):
                    all_keys.update(obj.keys())
            pattern.json_key_patterns = all_keys
            
            # Analyze common schema patterns
            schema_patterns = defaultdict(int)
            for obj in json_objects:
                if isinstance(obj, dict):
                    schema_key = tuple(sorted(obj.keys()))
                    schema_patterns[schema_key] += 1
            
            # Store most common schemas
            pattern.json_schemas = dict(Counter(schema_patterns).most_common(5))
    
    def _analyze_special_column_patterns(self, pattern: ColumnPattern, values: List[Any], column_name: str) -> None:
        """Analyze patterns for special column types based on name."""
        str_values = [str(v) for v in values if v is not None]
        
        # Email patterns
        if 'email' in column_name.lower():
            emails = [v for v in str_values if '@' in v]
            if emails:
                domains = [email.split('@')[-1] for email in emails]
                pattern.email_domains = [domain for domain, count in Counter(domains).most_common(10)]
        
        # URL patterns
        if 'url' in column_name.lower() or 'link' in column_name.lower():
            urls = [v for v in str_values if v.startswith(('http://', 'https://', 'ftp://'))]
            if urls:
                schemes = [url.split('://')[0] for url in urls]
                pattern.url_schemes = list(set(schemes))
                
                domains = []
                for url in urls:
                    try:
                        domain = url.split('://')[1].split('/')[0]
                        domains.append(domain)
                    except:
                        continue
                pattern.url_domains = [domain for domain, count in Counter(domains).most_common(10)]
        
        # Phone patterns
        if 'phone' in column_name.lower() or 'mobile' in column_name.lower():
            phones = [re.sub(r'[^\d+\-\(\)\s]', '', v) for v in str_values]
            phone_formats = []
            for phone in phones:
                # Detect format patterns
                format_pattern = re.sub(r'\d', 'X', phone)
                phone_formats.append(format_pattern)
            pattern.phone_formats = [fmt for fmt, count in Counter(phone_formats).most_common(5)]
    
    def _detect_regex_patterns(self, values: List[str]) -> List[str]:
        """Detect common regex patterns in string values."""
        patterns = []
        
        # Email pattern
        email_count = sum(1 for v in values if re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', v))
        if email_count > len(values) * 0.8:  # 80% are emails
            patterns.append(r'^[\w\.-]+@[\w\.-]+\.\w+$')
        
        # Phone pattern
        phone_count = sum(1 for v in values if re.match(r'^[\+\-\(\)\d\s]{10,}$', v))
        if phone_count > len(values) * 0.8:
            patterns.append(r'^[\+\-\(\)\d\s]{10,}$')
        
        # URL pattern
        url_count = sum(1 for v in values if re.match(r'^https?://', v))
        if url_count > len(values) * 0.8:
            patterns.append(r'^https?://.*')
        
        # Numeric string pattern
        numeric_count = sum(1 for v in values if v.isdigit())
        if numeric_count > len(values) * 0.8:
            patterns.append(r'^\d+$')
        
        return patterns
    
    def _analyze_foreign_key_patterns(self, table: TableInfo, sample_data: List[Dict[str, Any]]) -> Dict[str, Dict[Any, int]]:
        """Analyze foreign key value distributions."""
        fk_patterns = {}
        
        for fk in table.foreign_keys:
            if fk.columns and sample_data:
                fk_column = fk.columns[0]  # Assume single-column FK for simplicity
                fk_values = [row.get(fk_column) for row in sample_data if row.get(fk_column) is not None]
                fk_patterns[fk_column] = dict(Counter(fk_values))
        
        return fk_patterns
    
    def _analyze_correlations(self, table: TableInfo, sample_data: List[Dict[str, Any]]) -> Dict[Tuple[str, str], float]:
        """Analyze correlations between numeric columns."""
        correlations = {}
        
        numeric_columns = [col.name for col in table.columns 
                          if col.data_type.lower() in ['int', 'integer', 'bigint', 'smallint', 'decimal', 'float', 'double']]
        
        # Simple correlation analysis for numeric columns
        for i, col1 in enumerate(numeric_columns):
            for col2 in numeric_columns[i+1:]:
                try:
                    values1 = [float(row.get(col1, 0)) for row in sample_data if row.get(col1) is not None]
                    values2 = [float(row.get(col2, 0)) for row in sample_data if row.get(col2) is not None]
                    
                    if len(values1) == len(values2) and len(values1) > 1:
                        correlation = self._calculate_correlation(values1, values2)
                        if abs(correlation) > 0.3:  # Only store significant correlations
                            correlations[(col1, col2)] = correlation
                except:
                    continue
        
        return correlations
    
    def _calculate_correlation(self, x: List[float], y: List[float]) -> float:
        """Calculate Pearson correlation coefficient."""
        if len(x) != len(y) or len(x) < 2:
            return 0.0
        
        n = len(x)
        sum_x = sum(x)
        sum_y = sum(y)
        sum_xy = sum(xi * yi for xi, yi in zip(x, y))
        sum_x2 = sum(xi * xi for xi in x)
        sum_y2 = sum(yi * yi for yi in y)
        
        numerator = n * sum_xy - sum_x * sum_y
        denominator = ((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y)) ** 0.5
        
        if denominator == 0:
            return 0.0
        
        return numerator / denominator


class PatternBasedGenerator:
    """Generates data based on analyzed patterns."""
    
    def __init__(self, table_patterns: Dict[str, TablePattern]):
        """Initialize with analyzed table patterns."""
        self.table_patterns = table_patterns
    
    def generate_realistic_value(self, table_name: str, column_name: str, base_generator_func: callable) -> Any:
        """Generate a realistic value based on analyzed patterns."""
        if table_name not in self.table_patterns:
            return base_generator_func()
        
        table_pattern = self.table_patterns[table_name]
        if column_name not in table_pattern.column_patterns:
            return base_generator_func()
        
        column_pattern = table_pattern.column_patterns[column_name]
        
        # Use pattern-based generation
        return self._generate_from_pattern(column_pattern, base_generator_func)
    
    def _generate_from_pattern(self, pattern: ColumnPattern, base_generator_func: callable) -> Any:
        """Generate value based on specific column pattern."""
        import random
        
        # If we have common values and should use them (80% chance)
        if pattern.most_common_values and random.random() < 0.8:
            # Weight selection based on frequency
            total_count = sum(count for _, count in pattern.most_common_values)
            if total_count > 0:
                rand_val = random.randint(1, total_count)
            else:
                return base_generator_func()
            current_count = 0
            
            for value, count in pattern.most_common_values:
                current_count += count
                if rand_val <= current_count:
                    return value
        
        # Use pattern-specific generation
        if pattern.data_type.lower() in ['varchar', 'char', 'text', 'string']:
            return self._generate_string_from_pattern(pattern, base_generator_func)
        elif pattern.data_type.lower() in ['int', 'integer', 'bigint', 'smallint', 'decimal', 'float', 'double']:
            return self._generate_numeric_from_pattern(pattern, base_generator_func)
        elif pattern.data_type.lower() in ['datetime', 'timestamp', 'date']:
            return self._generate_date_from_pattern(pattern, base_generator_func)
        elif pattern.data_type.lower() in ['json', 'jsonb']:
            return self._generate_json_from_pattern(pattern, base_generator_func)
        
        # Fallback to base generator
        return base_generator_func()
    
    def _generate_string_from_pattern(self, pattern: ColumnPattern, base_generator_func: callable) -> str:
        """Generate string based on analyzed patterns."""
        import random
        from faker import Faker
        fake = Faker()
        
        # Use detected regex patterns
        if pattern.regex_patterns:
            pattern_type = pattern.regex_patterns[0]
            if 'email' in pattern_type:
                if pattern.email_domains:
                    domain = random.choice(pattern.email_domains)
                    return f"{fake.user_name()}@{domain}"
                return fake.email()
            elif 'phone' in pattern_type or r'[\+\-\(\)\d\s]' in pattern_type:
                if pattern.phone_formats:
                    format_template = random.choice(pattern.phone_formats)
                    # Replace X with random digits
                    return re.sub('X', lambda m: str(random.randint(0, 9)), format_template)
                return fake.phone_number()
            elif 'https?' in pattern_type:
                if pattern.url_domains:
                    domain = random.choice(pattern.url_domains)
                    return f"https://{domain}/{fake.uri_path()}"
                return fake.url()
        
        # Use length distribution
        if pattern.length_distribution:
            # Choose length based on distribution
            lengths = list(pattern.length_distribution.keys())
            weights = list(pattern.length_distribution.values())
            chosen_length = random.choices(lengths, weights=weights)[0]
            
            # Use prefixes/suffixes if available
            result = base_generator_func()
            if isinstance(result, str):
                if pattern.common_prefixes and random.random() < 0.3:
                    prefix = random.choice(pattern.common_prefixes)
                    result = prefix + result[len(prefix):]
                
                if pattern.common_suffixes and random.random() < 0.3:
                    suffix = random.choice(pattern.common_suffixes)
                    result = result[:-len(suffix)] + suffix
                
                # Adjust to target length
                if len(result) > chosen_length:
                    result = result[:chosen_length]
                elif len(result) < chosen_length:
                    result = result + fake.text(max_nb_chars=chosen_length - len(result))[:chosen_length - len(result)]
                
                return result
        
        return base_generator_func()
    
    def _generate_numeric_from_pattern(self, pattern: ColumnPattern, base_generator_func: callable) -> Union[int, float]:
        """Generate numeric value based on analyzed patterns."""
        import random
        
        if pattern.min_value is not None and pattern.max_value is not None:
            # Use normal distribution around mean if available
            if pattern.avg_value is not None and pattern.std_dev is not None:
                value = random.normalvariate(pattern.avg_value, pattern.std_dev)
                # Clamp to observed range
                value = max(pattern.min_value, min(pattern.max_value, value))
                return int(value) if pattern.data_type.lower() in ['int', 'integer', 'bigint', 'smallint'] else value
            else:
                # Uniform distribution in observed range
                if pattern.data_type.lower() in ['int', 'integer', 'bigint', 'smallint']:
                    min_val = int(pattern.min_value)
                    max_val = int(pattern.max_value)
                    
                    # Ensure min_val < max_val to avoid "low >= high" error
                    if min_val >= max_val:
                        max_val = min_val + 1000
                    
                    return random.randint(min_val, max_val)
                else:
                    min_val = float(pattern.min_value)
                    max_val = float(pattern.max_value)
                    
                    # Ensure min_val < max_val for uniform distribution
                    if min_val >= max_val:
                        max_val = min_val + 1000.0
                    
                    return random.uniform(min_val, max_val)
        
        return base_generator_func()
    
    def _generate_date_from_pattern(self, pattern: ColumnPattern, base_generator_func: callable) -> datetime:
        """Generate date based on analyzed patterns."""
        import random
        
        if pattern.date_range:
            start_date, end_date = pattern.date_range
            
            # Generate random date in range
            time_diff = end_date - start_date
            random_days = random.randint(0, time_diff.days)
            random_seconds = random.randint(0, 86400)  # seconds in a day
            
            result_date = start_date + timedelta(days=random_days, seconds=random_seconds)
            
            # Bias towards common day of week if available
            if pattern.common_day_of_week is not None and random.random() < 0.3:
                # Adjust to preferred day of week
                days_to_adjust = pattern.common_day_of_week - result_date.weekday()
                result_date += timedelta(days=days_to_adjust)
            
            # Bias towards common hour if available
            if pattern.common_hour is not None and random.random() < 0.3:
                result_date = result_date.replace(hour=pattern.common_hour)
            
            return result_date
        
        return base_generator_func()
    
    def _generate_json_from_pattern(self, pattern: ColumnPattern, base_generator_func: callable) -> str:
        """Generate JSON based on analyzed patterns."""
        import random
        from faker import Faker
        fake = Faker()
        
        if pattern.json_key_patterns:
            # Create object with keys from pattern
            json_obj = {}
            
            # Use common schema if available
            if pattern.json_schemas:
                schema_keys = random.choice(list(pattern.json_schemas.keys()))
                for key in schema_keys:
                    # Generate appropriate value based on key name
                    if 'id' in key.lower():
                        json_obj[key] = fake.random_int(min=1, max=10000)
                    elif 'name' in key.lower():
                        json_obj[key] = fake.name()
                    elif 'email' in key.lower():
                        json_obj[key] = fake.email()
                    elif 'date' in key.lower() or 'time' in key.lower():
                        json_obj[key] = fake.date_time().isoformat()
                    elif 'active' in key.lower():
                        json_obj[key] = fake.boolean()
                    elif 'value' in key.lower() or 'amount' in key.lower():
                        json_obj[key] = round(fake.random.uniform(1, 1000), 2)
                    else:
                        json_obj[key] = fake.word()
            else:
                # Use random selection of available keys
                selected_keys = random.sample(list(pattern.json_key_patterns), 
                                            min(5, len(pattern.json_key_patterns)))
                for key in selected_keys:
                    json_obj[key] = fake.word()
            
            return json.dumps(json_obj)
        
        return base_generator_func()
