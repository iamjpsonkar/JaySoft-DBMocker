"""
Enhanced data models with performance optimizations and advanced options.
"""

from typing import List, Dict, Any, Optional, Union, Callable
from enum import Enum
from dataclasses import dataclass, field
from pydantic import BaseModel, Field, validator


class PerformanceMode(Enum):
    """Performance optimization modes."""
    STANDARD = "standard"          # Standard generation
    HIGH_SPEED = "high_speed"      # Optimized for speed
    MEMORY_EFFICIENT = "memory_efficient"  # Optimized for memory
    BALANCED = "balanced"          # Balanced speed and memory
    ULTRA_HIGH = "ultra_high"      # Maximum performance for millions of records


class DuplicateStrategy(Enum):
    """Duplicate handling strategies."""
    GENERATE_NEW = "generate_new"        # Always generate new values
    ALLOW_SIMPLE = "allow_simple"       # Allow simple duplicates
    SMART_DUPLICATES = "smart_duplicates"  # Intelligent duplicate distribution
    CACHED_POOL = "cached_pool"         # Use cached value pools
    WEIGHTED_RANDOM = "weighted_random"  # Weighted random selection from pools


class CacheStrategy(Enum):
    """Caching strategies for performance."""
    NO_CACHE = "no_cache"
    SIMPLE_CACHE = "simple_cache"
    INTELLIGENT_CACHE = "intelligent_cache"
    ADAPTIVE_CACHE = "adaptive_cache"
    MEMORY_MAPPED = "memory_mapped"


class InsertionStrategy(Enum):
    """Database insertion strategies."""
    SINGLE_INSERT = "single_insert"
    BATCH_INSERT = "batch_insert"
    BULK_INSERT = "bulk_insert"
    STREAMING_INSERT = "streaming_insert"
    PARALLEL_BULK = "parallel_bulk"


@dataclass
class PerformanceSettings:
    """Performance configuration settings."""
    # Processing settings
    performance_mode: PerformanceMode = PerformanceMode.BALANCED
    max_workers: int = 4
    enable_multiprocessing: bool = False
    max_processes: int = 2
    
    # Memory settings
    max_memory_usage_percent: float = 80.0
    chunk_size_auto_adjust: bool = True
    min_chunk_size: int = 1000
    max_chunk_size: int = 100000
    
    # Cache settings
    cache_strategy: CacheStrategy = CacheStrategy.INTELLIGENT_CACHE
    cache_size_mb: int = 500
    cache_ttl_seconds: int = 3600
    
    # Database settings
    insertion_strategy: InsertionStrategy = InsertionStrategy.BULK_INSERT
    connection_pool_size: int = 10
    connection_pool_overflow: int = 5
    batch_size: int = 10000
    
    # Monitoring settings
    enable_progress_tracking: bool = True
    enable_performance_metrics: bool = True
    log_performance_every_n_rows: int = 10000


@dataclass
class DuplicateConfiguration:
    """Advanced duplicate handling configuration."""
    # Global duplicate settings
    global_duplicate_enabled: bool = False
    global_duplicate_strategy: DuplicateStrategy = DuplicateStrategy.GENERATE_NEW
    global_duplicate_probability: float = 0.3
    
    # Pool-based duplicates
    pool_size_small: int = 5      # For highly repeated values
    pool_size_medium: int = 20    # For moderately repeated values
    pool_size_large: int = 100    # For less repeated values
    
    # Smart duplicate distribution
    duplicate_distribution_weights: Dict[str, float] = field(default_factory=lambda: {
        "high_frequency": 0.6,    # 60% of rows use high-frequency values
        "medium_frequency": 0.3,  # 30% use medium-frequency values
        "unique": 0.1            # 10% are unique values
    })
    
    # Column-specific overrides
    column_duplicate_overrides: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    # Data reuse settings
    reuse_existing_data: bool = True
    reuse_probability: float = 0.4
    prefer_recent_values: bool = True


@dataclass
class OptimizationHints:
    """Optimization hints for data generation."""
    # Table characteristics
    is_large_table: bool = False
    has_complex_constraints: bool = False
    has_many_foreign_keys: bool = False
    
    # Data patterns
    has_repetitive_data: bool = False
    has_time_series_data: bool = False
    has_hierarchical_data: bool = False
    
    # Performance hints
    prioritize_speed: bool = False
    prioritize_memory: bool = False
    prioritize_quality: bool = True
    
    # Special handling
    use_precomputed_values: bool = False
    use_data_templates: bool = False
    use_pattern_matching: bool = False


class EnhancedGenerationConfig(BaseModel):
    """Enhanced configuration with performance and advanced features."""
    
    # Basic settings (from original)
    batch_size: int = Field(default=10000, description="Batch size for operations")
    max_workers: int = Field(default=4, description="Number of worker threads")
    seed: Optional[int] = Field(default=None, description="Random seed")
    truncate_existing: bool = Field(default=False, description="Truncate existing data")
    
    # Performance settings
    performance: PerformanceSettings = Field(default_factory=PerformanceSettings)
    
    # Duplicate handling
    duplicates: DuplicateConfiguration = Field(default_factory=DuplicateConfiguration)
    
    # Optimization hints
    optimization_hints: OptimizationHints = Field(default_factory=OptimizationHints)
    
    # Target scale settings
    target_rows_per_table: Dict[str, int] = Field(default_factory=dict)
    total_target_rows: Optional[int] = Field(default=None, description="Total rows to generate across all tables")
    
    # Quality settings
    data_quality_level: str = Field(default="standard", description="Data quality: basic, standard, high, realistic")
    maintain_referential_integrity: bool = Field(default=True)
    enforce_all_constraints: bool = Field(default=True)
    
    # Advanced generation modes
    generation_mode: str = Field(default="standard", description="Generation mode: standard, bulk, streaming, hybrid")
    
    # Table selection
    include_tables: Optional[List[str]] = Field(default=None)
    exclude_tables: List[str] = Field(default_factory=list)
    
    # Output and monitoring
    output_format: str = Field(default="database", description="Output format: database, json, csv, parquet")
    output_path: Optional[str] = Field(default=None)
    enable_statistics: bool = Field(default=True)
    enable_validation: bool = Field(default=True)
    
    # Compatibility with existing config
    table_configs: Dict[str, "EnhancedTableGenerationConfig"] = Field(default_factory=dict)
    
    @validator("generation_mode")
    def validate_generation_mode(cls, v):
        valid_modes = ["standard", "bulk", "streaming", "hybrid", "ultra_high_performance"]
        if v not in valid_modes:
            raise ValueError(f"Invalid generation mode: {v}. Valid modes: {valid_modes}")
        return v


class EnhancedTableGenerationConfig(BaseModel):
    """Enhanced table-specific configuration."""
    
    # Basic settings
    rows_to_generate: int = Field(default=1000, description="Number of rows to generate")
    use_existing_data: bool = Field(default=False, description="Use existing data")
    
    # Performance settings for this table
    table_performance: Optional[PerformanceSettings] = Field(default=None)
    
    # Duplicate settings for this table
    table_duplicates: Optional[DuplicateConfiguration] = Field(default=None)
    
    # Column configurations
    column_configs: Dict[str, "EnhancedColumnGenerationConfig"] = Field(default_factory=dict)
    
    # Table-specific hints
    optimization_hints: OptimizationHints = Field(default_factory=OptimizationHints)
    
    # Relationship handling
    foreign_key_strategy: str = Field(default="smart", description="FK strategy: random, smart, weighted, existing")
    foreign_key_reuse_probability: float = Field(default=0.7)
    
    # Data distribution
    data_distribution: Optional[Dict[str, Any]] = Field(default=None, description="Custom data distribution settings")


class EnhancedColumnGenerationConfig(BaseModel):
    """Enhanced column-specific configuration."""
    
    # Basic constraints (from original)
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    possible_values: Optional[List[Any]] = None
    null_probability: float = 0.0
    
    # Enhanced duplicate handling
    duplicate_strategy: DuplicateStrategy = DuplicateStrategy.GENERATE_NEW
    duplicate_pool_size: int = 10
    duplicate_weights: Optional[Dict[Any, float]] = None
    duplicate_probability: float = 0.3
    
    # Value generation strategies
    generation_strategy: str = Field(default="default", description="Generation strategy: default, pattern, lookup, computed")
    value_source: Optional[str] = Field(default=None, description="Source for values: faker, custom, file, existing")
    
    # Performance hints for this column
    is_high_cardinality: bool = False
    is_low_cardinality: bool = False
    cache_values: bool = True
    precompute_values: bool = False
    
    # Advanced value generation
    value_template: Optional[str] = Field(default=None, description="Template for value generation")
    computed_expression: Optional[str] = Field(default=None, description="Expression for computed values")
    lookup_table: Optional[str] = Field(default=None, description="Lookup table for values")
    
    # Relationship handling
    foreign_key_weight_distribution: Optional[Dict[Any, float]] = None
    
    # Custom generators
    custom_generator: Optional[str] = None
    generator_parameters: Dict[str, Any] = Field(default_factory=dict)


# Update forward references
EnhancedGenerationConfig.model_rebuild()
EnhancedTableGenerationConfig.model_rebuild()


@dataclass
class PerformanceReport:
    """Detailed performance report."""
    # Generation metrics
    total_rows_generated: int = 0
    total_time_seconds: float = 0.0
    average_rows_per_second: float = 0.0
    peak_rows_per_second: float = 0.0
    
    # Resource usage
    peak_memory_usage_mb: float = 0.0
    average_memory_usage_mb: float = 0.0
    peak_cpu_usage_percent: float = 0.0
    average_cpu_usage_percent: float = 0.0
    
    # Threading metrics
    threads_used: int = 0
    processes_used: int = 0
    total_thread_time_seconds: float = 0.0
    thread_efficiency_percent: float = 0.0
    
    # Cache performance
    cache_hit_rate: float = 0.0
    cache_size_mb: float = 0.0
    cache_evictions: int = 0
    
    # Database performance
    total_inserts: int = 0
    average_insert_time_ms: float = 0.0
    database_connection_pool_usage: float = 0.0
    
    # Error metrics
    generation_errors: int = 0
    insertion_errors: int = 0
    constraint_violations: int = 0
    
    # Quality metrics
    duplicate_rate: float = 0.0
    unique_value_rate: float = 0.0
    constraint_satisfaction_rate: float = 0.0
    
    # Table-specific metrics
    table_metrics: Dict[str, Dict[str, Any]] = field(default_factory=dict)


class BulkGenerationRequest(BaseModel):
    """Request for bulk data generation."""
    
    # Target specification
    tables: Dict[str, int] = Field(description="Table name to row count mapping")
    total_rows_limit: Optional[int] = Field(default=None, description="Maximum total rows across all tables")
    
    # Configuration
    config: EnhancedGenerationConfig = Field(default_factory=EnhancedGenerationConfig)
    
    # Execution options
    execution_mode: str = Field(default="parallel", description="Execution mode: sequential, parallel, streaming")
    priority_tables: List[str] = Field(default_factory=list, description="Tables to generate first")
    
    # Output options
    output_to_database: bool = Field(default=True)
    export_formats: List[str] = Field(default_factory=list, description="Export formats: json, csv, parquet")
    
    # Validation options
    validate_before_insert: bool = Field(default=False)
    validate_after_insert: bool = Field(default=True)
    
    # Monitoring options
    enable_real_time_monitoring: bool = Field(default=True)
    monitoring_interval_seconds: int = Field(default=30)
    
    @validator("execution_mode")
    def validate_execution_mode(cls, v):
        valid_modes = ["sequential", "parallel", "streaming", "hybrid"]
        if v not in valid_modes:
            raise ValueError(f"Invalid execution mode: {v}. Valid modes: {valid_modes}")
        return v


def create_high_performance_config(
    target_tables: Dict[str, int],
    performance_mode: PerformanceMode = PerformanceMode.HIGH_SPEED,
    enable_duplicates: bool = True,
    duplicate_strategy: DuplicateStrategy = DuplicateStrategy.SMART_DUPLICATES,
    **kwargs
) -> EnhancedGenerationConfig:
    """Create optimized configuration for high-performance generation."""
    
    # Performance settings based on mode
    perf_settings = PerformanceSettings()
    
    if performance_mode == PerformanceMode.ULTRA_HIGH:
        perf_settings.performance_mode = performance_mode
        perf_settings.max_workers = min(16, perf_settings.max_workers * 2)
        perf_settings.enable_multiprocessing = True
        perf_settings.max_processes = 4
        perf_settings.cache_strategy = CacheStrategy.MEMORY_MAPPED
        perf_settings.insertion_strategy = InsertionStrategy.PARALLEL_BULK
        perf_settings.batch_size = 50000
        perf_settings.connection_pool_size = 20
    elif performance_mode == PerformanceMode.HIGH_SPEED:
        perf_settings.performance_mode = performance_mode
        perf_settings.cache_strategy = CacheStrategy.INTELLIGENT_CACHE
        perf_settings.insertion_strategy = InsertionStrategy.BULK_INSERT
        perf_settings.batch_size = 25000
        perf_settings.connection_pool_size = 12
    elif performance_mode == PerformanceMode.MEMORY_EFFICIENT:
        perf_settings.performance_mode = performance_mode
        perf_settings.max_chunk_size = 10000
        perf_settings.cache_strategy = CacheStrategy.SIMPLE_CACHE
        perf_settings.cache_size_mb = 100
        perf_settings.insertion_strategy = InsertionStrategy.STREAMING_INSERT
    
    # Duplicate settings
    dup_config = DuplicateConfiguration()
    if enable_duplicates:
        dup_config.global_duplicate_enabled = True
        dup_config.global_duplicate_strategy = duplicate_strategy
        
        if duplicate_strategy == DuplicateStrategy.SMART_DUPLICATES:
            dup_config.pool_size_small = 5
            dup_config.pool_size_medium = 25
            dup_config.pool_size_large = 100
        elif duplicate_strategy == DuplicateStrategy.CACHED_POOL:
            dup_config.pool_size_small = 10
            dup_config.pool_size_medium = 50
            dup_config.pool_size_large = 200
    
    # Create enhanced config
    config = EnhancedGenerationConfig(
        target_rows_per_table=target_tables,
        performance=perf_settings,
        duplicates=dup_config,
        generation_mode="ultra_high_performance" if performance_mode == PerformanceMode.ULTRA_HIGH else "bulk",
        **kwargs
    )
    
    return config


def create_bulk_generation_request(
    tables: Dict[str, int],
    performance_mode: PerformanceMode = PerformanceMode.BALANCED,
    **kwargs
) -> BulkGenerationRequest:
    """Create bulk generation request with optimal settings."""
    
    config = create_high_performance_config(
        target_tables=tables,
        performance_mode=performance_mode,
        **kwargs
    )
    
    return BulkGenerationRequest(
        tables=tables,
        config=config,
        execution_mode="parallel" if performance_mode in [PerformanceMode.HIGH_SPEED, PerformanceMode.ULTRA_HIGH] else "sequential",
        enable_real_time_monitoring=True,
        validate_after_insert=True
    )
