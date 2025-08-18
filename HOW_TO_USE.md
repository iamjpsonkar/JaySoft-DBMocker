# DBMocker - Complete Usage Guide

## 🚀 Quick Start

DBMocker now supports ultra-fast data generation with advanced features including fast data reuse, multi-threading, and intelligent duplicate strategies. This guide covers all CLI commands and GUI options.

## 📋 Table of Contents

1. [Installation](#installation)
2. [CLI Commands](#cli-commands)
3. [GUI Usage](#gui-usage)
4. [Fast Data Reuse](#fast-data-reuse)
5. [Performance Modes](#performance-modes)
6. [Duplicate Strategies](#duplicate-strategies)
7. [Configuration Files](#configuration-files)
8. [Examples](#examples)

---

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Install DBMocker
pip install -e .
```

---

## CLI Commands

### Main Command Structure
```bash
python -m dbmocker.enhanced_cli [COMMAND] [OPTIONS]
```

### Available Commands

#### 1. `generate` - Generate Mock Data

**Basic Syntax:**
```bash
python -m dbmocker.enhanced_cli generate [OPTIONS]
```

**Database Connection Options:**
- `--host`, `-h`: Database host (default: "", not required for SQLite)
- `--port`, `-p`: Database port (default: 0, not required for SQLite)
- `--database`, `-d`: Database name (required)
- `--username`, `-u`: Database username (default: "", not required for SQLite)
- `--password`, `-w`: Database password (default: "", not required for SQLite)
- `--driver`: Database driver (`postgresql`, `mysql`, `sqlite`) (default: `postgresql`)

**Generation Options:**
- `--table`, `-t`: Specific table name (default: all tables)
- `--rows`, `-r`: Number of rows per table (default: 1000)
- `--performance-mode`: Performance optimization mode
  - `standard`: Basic generation
  - `high_speed`: Optimized for speed
  - `memory_efficient`: Optimized for memory
  - `balanced`: Balanced approach (default)
  - `ultra_high`: Maximum performance for millions of records

**Duplicate Strategy Options:**
- `--enable-duplicates`: Enable duplicate value generation
- `--duplicate-strategy`: Duplicate handling strategy
  - `generate_new`: Always generate unique values (default)
  - `allow_simple`: Allow simple duplicate values
  - `smart_duplicates`: Intelligent duplicate distribution
  - `cached_pool`: Use cached value pools
  - `fast_data_reuse`: **Ultra-fast reuse of existing data (fastest)**

**Fast Data Reuse Options:**
- `--sample-size`: Sample size for fast data reuse (default: 10000)
- `--reuse-probability`: Probability of reusing data (default: 0.95)
- `--progress-interval`: Progress update interval in rows (default: 1000)

**Performance Options:**
- `--batch-size`: Batch size for operations (auto-calculated if not set)
- `--max-workers`: Maximum number of worker threads (auto-calculated if not set)
- `--streaming`: Use streaming mode for very large datasets
- `--seed`: Random seed for reproducible data

**Output Options:**
- `--output-format`: Output format (`database`, `json`, `csv`) (default: `database`)
- `--output-path`: Output file path (for non-database formats)
- `--truncate`: Truncate tables before inserting data

**Utility Options:**
- `--config-file`: Configuration file path (JSON/YAML)
- `--dry-run`: Dry run - analyze only, do not generate data
- `--verbose`, `-v`: Enable verbose logging
- `--quiet`, `-q`: Quiet mode (warnings only)

#### 2. `analyze` - Analyze Database Schema

**Syntax:**
```bash
python -m dbmocker.enhanced_cli analyze [CONNECTION_OPTIONS]
```

**Additional Options:**
- `--table`, `-t`: Specific table name (default: all tables)
- `--output-format`: Output format (`table`, `json`, `yaml`) (default: `table`)

#### 3. `init-config` - Initialize Configuration File

**Syntax:**
```bash
python -m dbmocker.enhanced_cli init-config [OPTIONS]
```

**Options:**
- `--output`, `-o`: Output configuration file (default: `dbmocker_config.yaml`)
- `--format`: Configuration format (`yaml`, `json`) (default: `yaml`)
- `--performance-mode`: Performance optimization mode (default: `balanced`)
- `--include-examples`: Include example configurations

---

## CLI Examples

### Basic Examples

**Generate 1000 rows for all tables (PostgreSQL):**
```bash
python -m dbmocker.enhanced_cli generate \
  --host localhost \
  --port 5432 \
  --database mydb \
  --username user \
  --password password \
  --rows 1000
```

**Generate data for SQLite database:**
```bash
python -m dbmocker.enhanced_cli generate \
  --driver sqlite \
  --database mydata.db \
  --rows 5000
```

**Generate data for specific table:**
```bash
python -m dbmocker.enhanced_cli generate \
  --host localhost \
  --port 5432 \
  --database mydb \
  --username user \
  --password password \
  --table users \
  --rows 10000
```

### Ultra-Fast Data Reuse Examples

**Fast data reuse for 1 million records:**
```bash
python -m dbmocker.enhanced_cli generate \
  --driver sqlite \
  --database mydata.db \
  --table users \
  --rows 1000000 \
  --duplicate-strategy fast_data_reuse \
  --sample-size 50000 \
  --reuse-probability 0.95 \
  --progress-interval 1000 \
  --performance-mode ultra_high
```

**MySQL with fast data reuse:**
```bash
python -m dbmocker.enhanced_cli generate \
  --host localhost \
  --port 3306 \
  --database testdb \
  --username root \
  --password password \
  --driver mysql \
  --table orders \
  --rows 500000 \
  --duplicate-strategy fast_data_reuse \
  --sample-size 10000 \
  --reuse-probability 0.90 \
  --progress-interval 5000
```

### Advanced Examples

**High-performance generation with custom settings:**
```bash
python -m dbmocker.enhanced_cli generate \
  --host localhost \
  --port 5432 \
  --database mydb \
  --username user \
  --password password \
  --performance-mode ultra_high \
  --rows 1000000 \
  --enable-duplicates \
  --duplicate-strategy smart_duplicates \
  --streaming \
  --batch-size 50000 \
  --max-workers 16
```

**Using configuration file:**
```bash
# First create config
python -m dbmocker.enhanced_cli init-config \
  --performance-mode ultra_high \
  --include-examples

# Then use it
python -m dbmocker.enhanced_cli generate \
  --config-file dbmocker_config.yaml
```

**Analyze database schema:**
```bash
python -m dbmocker.enhanced_cli analyze \
  --host localhost \
  --port 5432 \
  --database mydb \
  --username user \
  --password password \
  --output-format json
```

---

## GUI Usage

### Starting the Enhanced GUI

**Method 1: Using the launcher script**
```bash
python run_enhanced_gui.py
```

**Method 2: Direct execution**
```bash
python -m dbmocker.gui.enhanced_main
```

### GUI Interface Overview

The enhanced GUI features a modern tabbed interface with 5 specialized tabs:

#### Tab 1: Database Connection
- **Driver Selection**: Choose between PostgreSQL, MySQL, SQLite
- **Connection Settings**: Host, port, database, credentials
- **Test Connection**: Verify database connectivity
- **Recent Connections**: Quick access to previous connections

#### Tab 2: Performance Configuration
- **Performance Mode Selection**:
  - 🔹 **Standard**: Basic generation (1K-10K rows)
  - 🔹 **High Speed**: Optimized for speed (10K-100K rows)
  - 🔹 **Memory Efficient**: Low memory usage
  - 🔹 **Balanced**: Balanced performance (default)
  - 🔹 **Ultra High**: Maximum performance (100K+ rows)

- **Advanced Settings**:
  - Max Workers: Number of parallel threads
  - Batch Size: Records per batch operation
  - Cache Settings: Memory allocation and strategy
  - Streaming Mode: For very large datasets

#### Tab 3: Duplicate Configuration
- **Enable Duplicates**: Toggle duplicate generation
- **Duplicate Strategy Selection**:
  - 🔹 **Generate New**: Always unique values
  - 🔹 **Allow Simple**: Simple duplicate values
  - 🔹 **Smart Duplicates**: Intelligent distribution
  - 🔹 **Cached Pool**: Cached value pools
  - 🔹 **Fast Data Reuse**: ⚡ Ultra-fast existing data reuse

- **Duplicate Settings**:
  - Probability: Chance of generating duplicates
  - Pool Sizes: Small, medium, large value pools

- **Fast Data Reuse Settings**:
  - Sample Size: Number of existing rows to sample (1K-100K)
  - Reuse Probability: Probability of reusing vs generating (0-100%)
  - Progress Interval: Update frequency (100-10K records)
  - Fast Insertion Mode: Enable fastest optimizations

#### Tab 4: Table Configuration
- **Table Selection**: Choose which tables to populate
- **Row Count Settings**: Set target rows per table
- **Bulk Operations**: Set same row count for multiple tables
- **Constraint Analysis**: View table constraints and relationships

#### Tab 5: Generation & Monitoring
- **Generation Mode Selection**:
  - 🔹 **Standard Generation**: Basic mode with auto-upgrades
  - 🔹 **High-Performance**: Optimized generation
  - 🔹 **Ultra-Fast Processing**: Maximum performance mode

- **Generation Options**:
  - Truncate Tables: Clear existing data first
  - Use Streaming: Memory-efficient mode
  - Random Seed: For reproducible results

- **Real-Time Monitoring**:
  - Progress bars with percentage completion
  - Current generation rate (rows/second)
  - ETA calculation
  - System resource usage (CPU, memory)
  - Live log output

### GUI Special Features

#### Auto-Optimization
- **Smart Mode Detection**: GUI automatically suggests ultra-fast mode when fast data reuse is selected
- **Performance Warnings**: Alerts for suboptimal configurations
- **Resource Monitoring**: Real-time CPU and memory usage

#### Progress Tracking
- **Every 1000 Records**: Configurable progress updates
- **Visual Indicators**: Progress bars, rate displays, ETA
- **Status Messages**: Real-time feedback on generation progress

#### Configuration Management
- **Save/Load Configurations**: Store frequently used settings
- **Export Settings**: Share configurations between team members
- **Validation**: Real-time parameter validation

---

## Fast Data Reuse

### What is Fast Data Reuse?

Fast Data Reuse is an ultra-high-performance feature that reuses existing data from your database instead of generating new values. This can achieve **149K+ rows/second** performance.

### How It Works

1. **Samples Existing Data**: Takes a configurable sample of existing records
2. **Analyzes Constraints**: Identifies which columns can be safely reused
3. **Reuses Safe Data**: Only reuses data that won't violate constraints
4. **Ultra-Fast Insertion**: Uses optimized bulk insertion techniques

### Constraint Safety

✅ **Safe to Reuse:**
- Regular columns (name, email, description, etc.)
- Foreign key columns
- Nullable columns

❌ **NOT Safe to Reuse:**
- Primary keys (auto-increment)
- Unique constraint columns
- Auto-increment columns

### Configuration Options

**Sample Size** (1,000 - 100,000):
- Number of existing rows to sample for reuse
- Larger samples = more variety
- Recommended: 10,000 for most use cases

**Reuse Probability** (0.0 - 1.0):
- Probability of reusing existing data vs generating new
- 0.95 = 95% reuse, 5% new generation
- Recommended: 0.90-0.95 for best performance

**Progress Interval** (100 - 10,000):
- How often to show progress updates
- Lower = more frequent updates
- Recommended: 1,000 for good balance

### Performance Expectations

| Dataset Size | Expected Time | Rate |
|-------------|---------------|------|
| 10K rows | ~0.1 seconds | 100K+ rows/sec |
| 100K rows | ~1 second | 100K+ rows/sec |
| 1M rows | ~10 seconds | 100K+ rows/sec |
| 10M rows | ~100 seconds | 100K+ rows/sec |

---

## Performance Modes

### Standard Mode
- **Use Case**: Small datasets (< 10K rows)
- **Features**: Basic generation with good compatibility
- **Performance**: 1K-5K rows/second

### High Speed Mode  
- **Use Case**: Medium datasets (10K-100K rows)
- **Features**: Optimized algorithms, intelligent caching
- **Performance**: 10K-50K rows/second

### Memory Efficient Mode
- **Use Case**: Limited memory environments
- **Features**: Streaming generation, minimal memory usage
- **Performance**: 5K-20K rows/second

### Balanced Mode (Default)
- **Use Case**: General purpose
- **Features**: Good balance of speed and memory usage
- **Performance**: 10K-30K rows/second

### Ultra High Mode
- **Use Case**: Large datasets (100K+ rows), fast data reuse
- **Features**: Maximum parallelization, advanced optimizations
- **Performance**: 50K-150K+ rows/second

---

## Duplicate Strategies

### Generate New (Default)
- Always generates unique values
- Slowest but most realistic
- Use for: Small datasets requiring uniqueness

### Allow Simple
- Allows simple duplicate values
- Faster than unique generation
- Use for: When some duplication is acceptable

### Smart Duplicates
- Intelligent duplicate distribution
- Realistic data patterns
- Use for: Balanced realism and performance

### Cached Pool
- Uses pre-generated value pools
- Very fast generation
- Use for: Large datasets with acceptable repetition

### Fast Data Reuse ⚡
- **Fastest option available**
- Reuses existing database data
- Use for: Ultra-fast insertion of millions of records

---

## Configuration Files

### Creating Configuration Files

```bash
# YAML format (default)
python -m dbmocker.enhanced_cli init-config \
  --output my_config.yaml \
  --performance-mode ultra_high \
  --include-examples

# JSON format
python -m dbmocker.enhanced_cli init-config \
  --output my_config.json \
  --format json \
  --performance-mode ultra_high
```

### Sample Configuration File

```yaml
# DBMocker Ultra-High Performance Configuration
database:
  driver: postgresql
  host: localhost
  port: 5432
  database: mydb
  username: user
  password: password

performance:
  performance_mode: ultra_high
  max_workers: 16
  batch_size: 50000
  enable_multiprocessing: true
  max_processes: 4
  cache_strategy: memory_mapped
  insertion_strategy: parallel_bulk

duplicates:
  global_duplicate_enabled: true
  global_duplicate_strategy: fast_data_reuse
  global_duplicate_probability: 0.95
  
  # Fast data reuse settings
  enable_fast_data_reuse: true
  data_reuse_sample_size: 50000
  data_reuse_probability: 0.95
  respect_constraints: true
  fast_insertion_mode: true
  progress_update_interval: 1000

generation:
  generation_mode: ultra_high_performance
  truncate_existing: false
  seed: 42

table_configs:
  users: 1000000
  orders: 500000
  products: 100000
```

### Using Configuration Files

```bash
python -m dbmocker.enhanced_cli generate --config-file my_config.yaml
```

---

## Examples

### Example 1: Small Project Setup
```bash
# SQLite database with 10K records
python -m dbmocker.enhanced_cli generate \
  --driver sqlite \
  --database project.db \
  --rows 10000 \
  --performance-mode balanced
```

### Example 2: Development Environment
```bash
# PostgreSQL with realistic duplicates
python -m dbmocker.enhanced_cli generate \
  --host localhost \
  --port 5432 \
  --database dev_db \
  --username dev_user \
  --password dev_pass \
  --rows 100000 \
  --enable-duplicates \
  --duplicate-strategy smart_duplicates \
  --performance-mode high_speed
```

### Example 3: Production-Scale Testing
```bash
# Ultra-fast data reuse for millions of records
python -m dbmocker.enhanced_cli generate \
  --host prod-test-server \
  --port 5432 \
  --database test_db \
  --username test_user \
  --password secure_pass \
  --table users \
  --rows 10000000 \
  --duplicate-strategy fast_data_reuse \
  --sample-size 100000 \
  --reuse-probability 0.95 \
  --progress-interval 10000 \
  --performance-mode ultra_high \
  --batch-size 100000 \
  --max-workers 32
```

### Example 4: Memory-Constrained Environment
```bash
# Optimized for low memory usage
python -m dbmocker.enhanced_cli generate \
  --host localhost \
  --port 3306 \
  --database mydb \
  --username user \
  --password pass \
  --driver mysql \
  --rows 1000000 \
  --performance-mode memory_efficient \
  --streaming \
  --batch-size 5000
```

---

## Tips and Best Practices

### For Maximum Performance
1. **Use Fast Data Reuse** for existing databases
2. **Choose Ultra High mode** for large datasets
3. **Increase batch size** for faster insertion
4. **Use appropriate sample sizes** (10K-50K for most cases)
5. **Monitor system resources** during generation

### For Memory Efficiency
1. **Use Memory Efficient mode** for large datasets
2. **Enable streaming mode** for minimal memory usage
3. **Reduce batch size** if memory is limited
4. **Lower worker count** to reduce memory usage

### For Realistic Data
1. **Use Smart Duplicates** for balanced realism
2. **Configure appropriate duplicate probabilities**
3. **Use cached pools** for consistent patterns
4. **Set random seeds** for reproducible results

### Troubleshooting
1. **Connection Issues**: Verify database credentials and network access
2. **Performance Issues**: Check system resources and adjust worker counts
3. **Memory Issues**: Use memory-efficient mode or reduce batch sizes
4. **Constraint Violations**: Ensure fast data reuse respects constraints

---

## Getting Help

```bash
# General help
python -m dbmocker.enhanced_cli --help

# Command-specific help
python -m dbmocker.enhanced_cli generate --help
python -m dbmocker.enhanced_cli analyze --help
python -m dbmocker.enhanced_cli init-config --help
```

## Performance Monitoring

Monitor your generation with real-time metrics:
- **Generation Rate**: Current rows/second
- **Progress**: Percentage and ETA
- **System Usage**: CPU and memory consumption
- **Database Load**: Connection and query performance

---

**Happy Data Generation! 🚀**

For more advanced usage and API documentation, see the source code and additional documentation files.
