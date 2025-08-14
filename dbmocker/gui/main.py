"""Main GUI application for DBMocker."""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog, scrolledtext
import threading
import queue
import logging
from typing import Optional, Dict, Any
import json

from dbmocker.core.database import DatabaseConnection, DatabaseConfig
from dbmocker.core.analyzer import SchemaAnalyzer
from dbmocker.core.generator import DataGenerator
from dbmocker.core.inserter import DataInserter
from dbmocker.core.models import GenerationConfig, TableGenerationConfig
from dbmocker.core.db_spec_analyzer import DatabaseSpecAnalyzer
from dbmocker.core.spec_driven_generator import SpecificationDrivenGenerator
from dbmocker.core.dependency_resolver import DependencyResolver, print_insertion_plan
from dbmocker.core.smart_generator import DependencyAwareGenerator, create_optimal_generation_config


class DBMockerGUI:
    """Main GUI application for JaySoft:DBMocker."""
    
    def __init__(self, root: tk.Tk):
        """Initialize the GUI application."""
        self.root = root
        self.root.title("JaySoft:DBMocker - Database Mock Data Generator")
        self.root.geometry("1000x700")
        
        # Application state
        self.db_connection: Optional[DatabaseConnection] = None
        self.schema = None
        self.generation_config = GenerationConfig()
        
        # Threading
        self.task_queue = queue.Queue()
        self.result_queue = queue.Queue()
        
        # Setup GUI
        self.setup_gui()
        self.setup_logging()
        
        # Start result processing
        self.process_results()
    
    def setup_gui(self):
        """Setup the GUI layout."""
        # Create main notebook for tabs
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Connection tab
        self.connection_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.connection_frame, text="Database Connection")
        self.setup_connection_tab()
        
        # Schema tab
        self.schema_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.schema_frame, text="Schema Analysis")
        self.setup_schema_tab()
        
        # Configuration tab
        self.config_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.config_frame, text="Generation Config")
        self.setup_config_tab()
        
        # Advanced Options tab
        self.advanced_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.advanced_frame, text="🔧 Advanced")
        self.setup_advanced_tab()
        
        # Generation tab
        self.generation_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.generation_frame, text="Data Generation")
        self.setup_generation_tab()
        
        # Logs tab
        self.logs_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.logs_frame, text="Logs")
        self.setup_logs_tab()
    
    def setup_connection_tab(self):
        """Setup database connection tab."""
        main_frame = ttk.Frame(self.connection_frame)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Title
        title_label = ttk.Label(main_frame, text="Database Connection", 
                               font=("Arial", 16, "bold"))
        title_label.pack(pady=(0, 20))
        
        # Connection form
        form_frame = ttk.LabelFrame(main_frame, text="Connection Details", padding=20)
        form_frame.pack(fill=tk.X, pady=(0, 20))
        
        # Database driver
        ttk.Label(form_frame, text="Database Type:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.driver_var = tk.StringVar(value="mysql")
        driver_combo = ttk.Combobox(form_frame, textvariable=self.driver_var, 
                                   values=["postgresql", "mysql", "sqlite"], state="readonly")
        driver_combo.grid(row=0, column=1, sticky=tk.EW, pady=5, padx=(10, 0))
        
        # Host
        ttk.Label(form_frame, text="Host:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.host_var = tk.StringVar(value="localhost")
        ttk.Entry(form_frame, textvariable=self.host_var).grid(row=1, column=1, sticky=tk.EW, pady=5, padx=(10, 0))
        
        # Port
        ttk.Label(form_frame, text="Port:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.port_var = tk.StringVar(value="3306")
        ttk.Entry(form_frame, textvariable=self.port_var).grid(row=2, column=1, sticky=tk.EW, pady=5, padx=(10, 0))
        
        # Database
        ttk.Label(form_frame, text="Database:").grid(row=3, column=0, sticky=tk.W, pady=5)
        self.database_var = tk.StringVar()
        ttk.Entry(form_frame, textvariable=self.database_var).grid(row=3, column=1, sticky=tk.EW, pady=5, padx=(10, 0))
        
        # Username
        ttk.Label(form_frame, text="Username:").grid(row=4, column=0, sticky=tk.W, pady=5)
        self.username_var = tk.StringVar(value="root")
        ttk.Entry(form_frame, textvariable=self.username_var).grid(row=4, column=1, sticky=tk.EW, pady=5, padx=(10, 0))
        
        # Password
        ttk.Label(form_frame, text="Password:").grid(row=5, column=0, sticky=tk.W, pady=5)
        self.password_var = tk.StringVar(value="")
        ttk.Entry(form_frame, textvariable=self.password_var, show="*").grid(row=5, column=1, sticky=tk.EW, pady=5, padx=(10, 0))
        
        form_frame.columnconfigure(1, weight=1)
        
        # Buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X)
        
        self.connect_button = ttk.Button(button_frame, text="Test Connection", 
                                        command=self.test_connection)
        self.connect_button.pack(side=tk.LEFT)
        
        self.analyze_button = ttk.Button(button_frame, text="Connect & Analyze Schema", 
                                        command=self.connect_and_analyze, state=tk.DISABLED)
        self.analyze_button.pack(side=tk.LEFT, padx=(10, 0))
        
        # Status
        self.connection_status = ttk.Label(main_frame, text="Not connected", foreground="red")
        self.connection_status.pack(pady=(10, 0))
    
    def setup_schema_tab(self):
        """Setup schema analysis tab."""
        main_frame = ttk.Frame(self.schema_frame)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Title
        title_label = ttk.Label(main_frame, text="Database Schema", 
                               font=("Arial", 16, "bold"))
        title_label.pack(pady=(0, 20))
        
        # Table list
        table_frame = ttk.LabelFrame(main_frame, text="Tables", padding=10)
        table_frame.pack(fill=tk.BOTH, expand=True)
        
        # Treeview for tables
        columns = ("name", "rows", "columns", "foreign_keys")
        self.table_tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=15)
        
        # Define column headings
        self.table_tree.heading("name", text="Table Name")
        self.table_tree.heading("rows", text="Current Rows")
        self.table_tree.heading("columns", text="Columns")
        self.table_tree.heading("foreign_keys", text="Foreign Keys")
        
        # Configure column widths
        self.table_tree.column("name", width=200)
        self.table_tree.column("rows", width=100)
        self.table_tree.column("columns", width=100)
        self.table_tree.column("foreign_keys", width=100)
        
        # Scrollbar
        scrollbar = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.table_tree.yview)
        self.table_tree.configure(yscrollcommand=scrollbar.set)
        
        self.table_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Bind selection event
        self.table_tree.bind("<<TreeviewSelect>>", self.on_table_select)
    
    def setup_config_tab(self):
        """Setup generation configuration tab."""
        main_frame = ttk.Frame(self.config_frame)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Title
        title_label = ttk.Label(main_frame, text="Generation Configuration", 
                               font=("Arial", 16, "bold"))
        title_label.pack(pady=(0, 20))
        
        # Global settings
        global_frame = ttk.LabelFrame(main_frame, text="Global Settings", padding=20)
        global_frame.pack(fill=tk.X, pady=(0, 20))
        
        # Batch size
        ttk.Label(global_frame, text="Batch Size:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.batch_size_var = tk.StringVar(value="1000")
        ttk.Entry(global_frame, textvariable=self.batch_size_var, width=10).grid(row=0, column=1, sticky=tk.W, pady=5, padx=(10, 0))
        
        # Random seed
        ttk.Label(global_frame, text="Random Seed:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.seed_var = tk.StringVar()
        ttk.Entry(global_frame, textvariable=self.seed_var, width=10).grid(row=1, column=1, sticky=tk.W, pady=5, padx=(10, 0))
        
        # Truncate existing
        self.truncate_var = tk.BooleanVar()
        ttk.Checkbutton(global_frame, text="Truncate existing data before insert", 
                       variable=self.truncate_var).grid(row=2, column=0, columnspan=2, sticky=tk.W, pady=5)
        
        # Table-specific settings
        table_config_frame = ttk.LabelFrame(main_frame, text="Table Configuration", padding=20)
        table_config_frame.pack(fill=tk.BOTH, expand=True)
        
        # Table selection and row count
        control_frame = ttk.Frame(table_config_frame)
        control_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(control_frame, text="Rows to generate per table:").pack(side=tk.LEFT)
        self.default_rows_var = tk.StringVar(value="1000")
        ttk.Entry(control_frame, textvariable=self.default_rows_var, width=10).pack(side=tk.LEFT, padx=(10, 0))
        
        ttk.Button(control_frame, text="Apply to All Tables", 
                  command=self.apply_default_rows).pack(side=tk.RIGHT)
        
        # Table configuration tree
        config_columns = ("table", "rows", "status")
        self.config_tree = ttk.Treeview(table_config_frame, columns=config_columns, show="headings", height=10)
        
        self.config_tree.heading("table", text="Table Name")
        self.config_tree.heading("rows", text="Rows to Generate")
        self.config_tree.heading("status", text="Status")
        
        self.config_tree.column("table", width=200)
        self.config_tree.column("rows", width=150)
        self.config_tree.column("status", width=100)
        
        config_scrollbar = ttk.Scrollbar(table_config_frame, orient=tk.VERTICAL, command=self.config_tree.yview)
        self.config_tree.configure(yscrollcommand=config_scrollbar.set)
        
        self.config_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        config_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    
    def setup_generation_tab(self):
        """Setup data generation tab."""
        main_frame = ttk.Frame(self.generation_frame)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Title
        title_label = ttk.Label(main_frame, text="Data Generation", 
                               font=("Arial", 16, "bold"))
        title_label.pack(pady=(0, 20))
        
        # Controls
        controls_frame = ttk.Frame(main_frame)
        controls_frame.pack(fill=tk.X, pady=(0, 20))
        
        self.generate_button = ttk.Button(controls_frame, text="🎲 Generate Data", 
                                         command=self.start_generation, state=tk.DISABLED)
        self.generate_button.pack(side=tk.LEFT)
        
        self.dry_run_var = tk.BooleanVar()
        ttk.Checkbutton(controls_frame, text="Dry Run (don't insert)", 
                       variable=self.dry_run_var).pack(side=tk.LEFT, padx=(20, 0))
        
        self.verify_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(controls_frame, text="Verify integrity", 
                       variable=self.verify_var).pack(side=tk.LEFT, padx=(20, 0))
        
        self.spec_driven_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(controls_frame, text="🔍 Spec-driven (DESCRIBE-based)", 
                       variable=self.spec_driven_var).pack(side=tk.LEFT, padx=(20, 0))
        
        self.dependency_aware_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(controls_frame, text="🔗 Dependency-aware order", 
                       variable=self.dependency_aware_var).pack(side=tk.LEFT, padx=(20, 0))
        
        # Progress
        progress_frame = ttk.LabelFrame(main_frame, text="Progress", padding=20)
        progress_frame.pack(fill=tk.X, pady=(0, 20))
        
        self.progress_label = ttk.Label(progress_frame, text="Ready to generate data")
        self.progress_label.pack()
        
        self.progress_bar = ttk.Progressbar(progress_frame, mode='indeterminate')
        self.progress_bar.pack(fill=tk.X, pady=(10, 0))
        
        # Results
        results_frame = ttk.LabelFrame(main_frame, text="Generation Results", padding=20)
        results_frame.pack(fill=tk.BOTH, expand=True)
        
        self.results_text = scrolledtext.ScrolledText(results_frame, height=10, state=tk.DISABLED)
        self.results_text.pack(fill=tk.BOTH, expand=True)
    
    def setup_logs_tab(self):
        """Setup logs tab."""
        main_frame = ttk.Frame(self.logs_frame)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Title
        title_label = ttk.Label(main_frame, text="Application Logs", 
                               font=("Arial", 16, "bold"))
        title_label.pack(pady=(0, 20))
        
        # Log controls
        controls_frame = ttk.Frame(main_frame)
        controls_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Button(controls_frame, text="Clear Logs", command=self.clear_logs).pack(side=tk.LEFT)
        ttk.Button(controls_frame, text="Save Logs", command=self.save_logs).pack(side=tk.LEFT, padx=(10, 0))
        
        # Log display
        self.log_text = scrolledtext.ScrolledText(main_frame, height=20, state=tk.DISABLED)
        self.log_text.pack(fill=tk.BOTH, expand=True)
    
    def setup_logging(self):
        """Setup logging to display in GUI."""
        self.log_handler = GUILogHandler(self.log_text)
        self.log_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', 
                                     datefmt='%H:%M:%S')
        self.log_handler.setFormatter(formatter)
        
        # Add handler to root logger
        logging.getLogger().addHandler(self.log_handler)
    
    def test_connection(self):
        """Test database connection."""
        try:
            config = self.get_db_config()
            with DatabaseConnection(config) as db_conn:
                if db_conn.test_connection():
                    self.connection_status.config(text="✅ Connection successful", foreground="green")
                    self.analyze_button.config(state=tk.NORMAL)
                    messagebox.showinfo("Success", "Database connection successful!")
                else:
                    raise ConnectionError("Connection test failed")
        except Exception as e:
            self.connection_status.config(text="❌ Connection failed", foreground="red")
            self.analyze_button.config(state=tk.DISABLED)
            messagebox.showerror("Connection Error", str(e))
    
    def connect_and_analyze(self):
        """Connect to database and analyze schema."""
        def analyze_task():
            try:
                config = self.get_db_config()
                self.db_connection = DatabaseConnection(config)
                self.db_connection.connect()
                
                analyzer = SchemaAnalyzer(self.db_connection)
                self.schema = analyzer.analyze_schema(analyze_data_patterns=True)
                
                self.result_queue.put(("schema_analyzed", self.schema))
            except Exception as e:
                self.result_queue.put(("error", str(e)))
        
        # Disable button and start analysis
        self.analyze_button.config(state=tk.DISABLED, text="Analyzing...")
        thread = threading.Thread(target=analyze_task)
        thread.daemon = True
        thread.start()
    
    def populate_schema_tab(self, schema):
        """Populate schema tab with analysis results."""
        # Clear existing items
        self.table_tree.delete(*self.table_tree.get_children())
        
        # Add tables
        for table in schema.tables:
            self.table_tree.insert("", tk.END, values=(
                table.name,
                f"{table.row_count:,}",
                len(table.columns),
                len(table.foreign_keys)
            ))
        
        # Populate configuration tab
        self.populate_config_tab(schema)
        
        # Enable generation
        self.generate_button.config(state=tk.NORMAL)
    
    def populate_config_tab(self, schema):
        """Populate configuration tab with table settings."""
        # Clear existing items
        self.config_tree.delete(*self.config_tree.get_children())
        
        # Add tables with default configuration
        for table in schema.tables:
            self.config_tree.insert("", tk.END, values=(
                table.name,
                self.default_rows_var.get(),
                "Ready"
            ))
    
    def apply_default_rows(self):
        """Apply default row count to all tables."""
        default_rows = self.default_rows_var.get()
        for item in self.config_tree.get_children():
            values = list(self.config_tree.item(item, "values"))
            values[1] = default_rows
            self.config_tree.item(item, values=values)
    
    def setup_advanced_tab(self):
        """Setup advanced options tab with all CLI features."""
        main_frame = ttk.Frame(self.advanced_frame)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Title
        title_label = ttk.Label(main_frame, text="Advanced Options", 
                               font=("Arial", 16, "bold"))
        title_label.pack(pady=(0, 20))
        
        # Create scrollable frame for all options
        canvas = tk.Canvas(main_frame)
        scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Table Filtering Options
        filter_frame = ttk.LabelFrame(scrollable_frame, text="Table Filtering", padding=10)
        filter_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(filter_frame, text="Include Tables:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.include_tables_var = tk.StringVar()
        ttk.Entry(filter_frame, textvariable=self.include_tables_var, width=50).grid(row=0, column=1, sticky=tk.EW, pady=5, padx=(10, 0))
        ttk.Label(filter_frame, text="(comma-separated)").grid(row=0, column=2, sticky=tk.W, pady=5, padx=(5, 0))
        
        ttk.Label(filter_frame, text="Exclude Tables:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.exclude_tables_var = tk.StringVar()
        ttk.Entry(filter_frame, textvariable=self.exclude_tables_var, width=50).grid(row=1, column=1, sticky=tk.EW, pady=5, padx=(10, 0))
        ttk.Label(filter_frame, text="(comma-separated)").grid(row=1, column=2, sticky=tk.W, pady=5, padx=(5, 0))
        
        filter_frame.columnconfigure(1, weight=1)
        
        # Analysis Options
        analysis_frame = ttk.LabelFrame(scrollable_frame, text="Schema Analysis Options", padding=10)
        analysis_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.analyze_patterns_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(analysis_frame, text="Analyze existing data patterns", 
                       variable=self.analyze_patterns_var).pack(anchor=tk.W, pady=2)
        
        self.show_specs_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(analysis_frame, text="Show detailed table specifications", 
                       variable=self.show_specs_var).pack(anchor=tk.W, pady=2)
        
        # Pattern Analysis Options
        pattern_analysis_frame = ttk.LabelFrame(scrollable_frame, text="🎯 Pattern-Based Generation", padding=10)
        pattern_analysis_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.analyze_existing_data_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(pattern_analysis_frame, text="🔍 Analyze existing data for realistic patterns", 
                       variable=self.analyze_existing_data_var, command=self.toggle_pattern_options).pack(anchor=tk.W, pady=2)
        
        ttk.Label(pattern_analysis_frame, text="Generates realistic data based on existing records in your tables", 
                 font=("Arial", 9), foreground="gray").pack(anchor=tk.W, pady=(0, 5))
        
        # Pattern sample size
        pattern_sample_frame = ttk.Frame(pattern_analysis_frame)
        pattern_sample_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(pattern_sample_frame, text="Sample Size:").pack(side=tk.LEFT)
        self.pattern_sample_size_var = tk.StringVar(value="1000")
        pattern_sample_entry = ttk.Entry(pattern_sample_frame, textvariable=self.pattern_sample_size_var, width=10)
        pattern_sample_entry.pack(side=tk.LEFT, padx=(10, 5))
        ttk.Label(pattern_sample_frame, text="records to analyze per table").pack(side=tk.LEFT)
        
        # Store pattern sample entry for enabling/disabling
        self.pattern_sample_entry = pattern_sample_entry
        
        # Random Seed Options
        seed_frame = ttk.LabelFrame(scrollable_frame, text="Random Seed Options", padding=10)
        seed_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.use_seed_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(seed_frame, text="Use random seed for reproducible data", 
                       variable=self.use_seed_var, command=self.toggle_seed_entry).pack(anchor=tk.W, pady=2)
        
        seed_input_frame = ttk.Frame(seed_frame)
        seed_input_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(seed_input_frame, text="Seed Value:").pack(side=tk.LEFT)
        self.seed_var = tk.StringVar(value="42")
        self.seed_entry = ttk.Entry(seed_input_frame, textvariable=self.seed_var, width=20, state=tk.DISABLED)
        self.seed_entry.pack(side=tk.LEFT, padx=(10, 0))
        
        # Config File Options
        config_frame = ttk.LabelFrame(scrollable_frame, text="Configuration File Management", padding=10)
        config_frame.pack(fill=tk.X, pady=(0, 10))
        
        config_buttons_frame = ttk.Frame(config_frame)
        config_buttons_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(config_buttons_frame, text="📂 Load Config", 
                  command=self.load_config_file).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(config_buttons_frame, text="💾 Save Config", 
                  command=self.save_config_file).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(config_buttons_frame, text="🔧 Generate Template", 
                  command=self.generate_config_template).pack(side=tk.LEFT)
        
        # Display config file path
        self.config_file_var = tk.StringVar(value="No config file loaded")
        ttk.Label(config_frame, textvariable=self.config_file_var, foreground="gray").pack(anchor=tk.W, pady=(5, 0))
        
        # Auto Configuration
        auto_config_frame = ttk.LabelFrame(scrollable_frame, text="Smart Auto-Configuration", padding=10)
        auto_config_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.auto_config_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(auto_config_frame, text="🤖 Generate optimal configuration automatically", 
                       variable=self.auto_config_var).pack(anchor=tk.W, pady=2)
        
        ttk.Label(auto_config_frame, text="Auto-config analyzes your schema and creates optimal generation rules", 
                 font=("Arial", 9), foreground="gray").pack(anchor=tk.W, pady=(0, 5))
        
        # Advanced Generation Options
        advanced_gen_frame = ttk.LabelFrame(scrollable_frame, text="Advanced Generation", padding=10)
        advanced_gen_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.show_dependency_plan_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(advanced_gen_frame, text="📋 Show dependency insertion plan", 
                       variable=self.show_dependency_plan_var).pack(anchor=tk.W, pady=2)
        
        # Pack canvas and scrollbar
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Bind mousewheel to canvas
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        canvas.bind_all("<MouseWheel>", _on_mousewheel)
    
    def toggle_seed_entry(self):
        """Toggle seed entry based on checkbox."""
        if self.use_seed_var.get():
            self.seed_entry.config(state=tk.NORMAL)
        else:
            self.seed_entry.config(state=tk.DISABLED)
    
    def load_config_file(self):
        """Load configuration from file."""
        from tkinter import filedialog
        
        file_path = filedialog.askopenfilename(
            title="Load Configuration File",
            filetypes=[
                ("YAML files", "*.yaml *.yml"),
                ("JSON files", "*.json"),
                ("All files", "*.*")
            ]
        )
        
        if file_path:
            try:
                import yaml
                import json
                from pathlib import Path
                
                config_file = Path(file_path)
                
                with open(config_file, 'r') as f:
                    if config_file.suffix.lower() == '.json':
                        config_data = json.load(f)
                    else:
                        config_data = yaml.safe_load(f)
                
                # Apply loaded configuration to GUI
                self.apply_config_to_gui(config_data)
                self.config_file_var.set(f"Loaded: {config_file.name}")
                messagebox.showinfo("Success", f"Configuration loaded from {config_file.name}")
                
            except Exception as e:
                messagebox.showerror("Error", f"Failed to load configuration: {e}")
    
    def save_config_file(self):
        """Save current GUI configuration to file."""
        from tkinter import filedialog
        
        file_path = filedialog.asksaveasfilename(
            title="Save Configuration File",
            defaultextension=".yaml",
            filetypes=[
                ("YAML files", "*.yaml"),
                ("JSON files", "*.json"),
                ("All files", "*.*")
            ]
        )
        
        if file_path:
            try:
                config_data = self.extract_config_from_gui()
                
                import yaml
                import json
                from pathlib import Path
                
                config_file = Path(file_path)
                
                with open(config_file, 'w') as f:
                    if config_file.suffix.lower() == '.json':
                        json.dump(config_data, f, indent=2)
                    else:
                        yaml.dump(config_data, f, default_flow_style=False, indent=2)
                
                self.config_file_var.set(f"Saved: {config_file.name}")
                messagebox.showinfo("Success", f"Configuration saved to {config_file.name}")
                
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save configuration: {e}")
    
    def generate_config_template(self):
        """Generate a configuration template."""
        from tkinter import filedialog
        
        file_path = filedialog.asksaveasfilename(
            title="Generate Configuration Template",
            defaultextension=".yaml",
            filetypes=[
                ("YAML files", "*.yaml"),
                ("JSON files", "*.json")
            ]
        )
        
        if file_path:
            try:
                import yaml
                from pathlib import Path
                
                # Create template configuration
                template = {
                    'generation_config': {
                        'batch_size': 1000,
                        'seed': 42,
                        'truncate_existing': False
                    },
                    'table_configs': {
                        'example_table': {
                            'rows_to_generate': 1000,
                            'column_configs': {
                                'example_column': {
                                    'min_value': 1,
                                    'max_value': 100,
                                    'null_probability': 0.1
                                }
                            }
                        }
                    }
                }
                
                config_file = Path(file_path)
                
                with open(config_file, 'w') as f:
                    if config_file.suffix.lower() == '.json':
                        import json
                        json.dump(template, f, indent=2)
                    else:
                        yaml.dump(template, f, default_flow_style=False, indent=2)
                
                messagebox.showinfo("Success", f"Configuration template created at {config_file.name}")
                
            except Exception as e:
                messagebox.showerror("Error", f"Failed to create template: {e}")
    
    def apply_config_to_gui(self, config_data):
        """Apply configuration data to GUI elements."""
        # Apply generation config
        if 'generation_config' in config_data:
            gen_config = config_data['generation_config']
            if 'batch_size' in gen_config:
                self.batch_size_var.set(str(gen_config['batch_size']))
            if 'truncate_existing' in gen_config:
                self.truncate_var.set(gen_config['truncate_existing'])
            if 'seed' in gen_config:
                self.seed_var.set(str(gen_config['seed']))
                self.use_seed_var.set(True)
                self.toggle_seed_entry()
        
        # Apply table configs to tree
        table_configs = config_data.get('table_configs', config_data.get('tables', {}))
        if table_configs and hasattr(self, 'config_tree'):
            for item in self.config_tree.get_children():
                values = list(self.config_tree.item(item, "values"))
                table_name = values[0]
                
                if table_name in table_configs:
                    table_config = table_configs[table_name]
                    if 'rows_to_generate' in table_config:
                        values[1] = table_config['rows_to_generate']
                        self.config_tree.item(item, values=values)
    
    def extract_config_from_gui(self):
        """Extract configuration from GUI elements."""
        config = {
            'generation_config': {
                'batch_size': int(self.batch_size_var.get()),
                'truncate_existing': self.truncate_var.get()
            },
            'table_configs': {}
        }
        
        # Add seed if enabled
        if self.use_seed_var.get():
            try:
                config['generation_config']['seed'] = int(self.seed_var.get())
            except ValueError:
                pass
        
        # Extract table configurations
        if hasattr(self, 'config_tree'):
            for item in self.config_tree.get_children():
                values = self.config_tree.item(item, "values")
                table_name = values[0]
                rows_to_generate = int(values[1])
                
                if rows_to_generate > 0:
                    config['table_configs'][table_name] = {
                        'rows_to_generate': rows_to_generate
                    }
        
        return config
    
    def build_generation_config(self):
        """Build generation configuration from GUI settings."""
        # Start with base config
        config = GenerationConfig(
            batch_size=int(self.batch_size_var.get()),
            truncate_existing=self.truncate_var.get()
        )
        
        # Add seed if enabled
        if hasattr(self, 'use_seed_var') and self.use_seed_var.get():
            try:
                config.seed = int(self.seed_var.get())
            except (ValueError, AttributeError):
                pass
        
        # Add table filtering if specified
        if hasattr(self, 'include_tables_var') and self.include_tables_var.get().strip():
            include_tables = [t.strip() for t in self.include_tables_var.get().split(',') if t.strip()]
            config.include_tables = include_tables
        
        if hasattr(self, 'exclude_tables_var') and self.exclude_tables_var.get().strip():
            exclude_tables = [t.strip() for t in self.exclude_tables_var.get().split(',') if t.strip()]
            config.exclude_tables = exclude_tables
        
        # Add pattern analysis options if available
        if hasattr(self, 'analyze_existing_data_var'):
            config.analyze_existing_data = self.analyze_existing_data_var.get()
        
        if hasattr(self, 'pattern_sample_size_var'):
            try:
                config.pattern_sample_size = int(self.pattern_sample_size_var.get())
            except (ValueError, AttributeError):
                config.pattern_sample_size = 1000
        
        # Extract table configurations from tree
        if hasattr(self, 'config_tree'):
            for item in self.config_tree.get_children():
                values = self.config_tree.item(item, "values")
                table_name = values[0]
                rows_to_generate = int(values[1])
                
                if rows_to_generate > 0:
                    config.table_configs[table_name] = TableGenerationConfig(
                        rows_to_generate=rows_to_generate
                    )
        
        return config
    
    def start_generation(self):
        """Start data generation process."""
        def generation_task():
            try:
                # Set random seed if enabled
                if hasattr(self, 'use_seed_var') and self.use_seed_var.get():
                    try:
                        import random
                        seed_value = int(self.seed_var.get())
                        random.seed(seed_value)
                        self.result_queue.put(("progress", f"🎲 Using random seed: {seed_value}"))
                    except (ValueError, AttributeError):
                        pass
                
                # Build generation config
                config = self.build_generation_config()
                
                # Step 1: Analyze database dependencies
                if self.dependency_aware_var.get():
                    self.result_queue.put(("progress", "🔗 Analyzing table dependencies..."))
                    
                    dependency_resolver = DependencyResolver(self.schema)
                    insertion_plan = dependency_resolver.create_insertion_plan()
                    
                    self.result_queue.put(("progress", f"✅ Created dependency plan with {len(insertion_plan.get_insertion_batches())} batches"))
                    
                    # Show dependency plan if requested
                    if hasattr(self, 'show_dependency_plan_var') and self.show_dependency_plan_var.get():
                        batches = insertion_plan.get_insertion_batches()
                        plan_text = f"\n📋 DEPENDENCY INSERTION PLAN:\n"
                        for i, batch in enumerate(batches, 1):
                            plan_text += f"  Batch {i}: {', '.join(batch)}\n"
                        self.result_queue.put(("progress", plan_text))
                else:
                    insertion_plan = None
                
                # Step 2: Check if specification-driven generation is enabled
                if self.spec_driven_var.get():
                    # Analyze database specifications using DESCRIBE
                    self.result_queue.put(("progress", "🔍 Analyzing database specifications using DESCRIBE..."))
                    
                    spec_analyzer = DatabaseSpecAnalyzer(self.db_connection)
                    
                    # Use same table filtering as schema analysis
                    include_tables = None
                    exclude_tables = None
                    
                    if hasattr(self, 'include_tables_var') and self.include_tables_var.get().strip():
                        include_tables = [t.strip() for t in self.include_tables_var.get().split(',') if t.strip()]
                    
                    if hasattr(self, 'exclude_tables_var') and self.exclude_tables_var.get().strip():
                        exclude_tables = [t.strip() for t in self.exclude_tables_var.get().split(',') if t.strip()]
                    
                    # Also limit to tables that exist in schema
                    schema_table_names = [table.name for table in self.schema.tables]
                    if include_tables:
                        include_tables = [t for t in include_tables if t in schema_table_names]
                    else:
                        include_tables = schema_table_names
                    
                    table_specs = spec_analyzer.analyze_all_tables(
                        include_tables=include_tables,
                        exclude_tables=exclude_tables
                    )
                    
                    if not table_specs:
                        self.result_queue.put(("error", "Failed to analyze database specifications"))
                        return
                    
                    self.result_queue.put(("progress", f"✅ Analyzed {len(table_specs)} tables with exact specifications"))
                    
                    # Show table specifications if requested
                    if hasattr(self, 'show_specs_var') and self.show_specs_var.get():
                        specs_text = f"\n🔍 TABLE SPECIFICATIONS SUMMARY:\n"
                        for table_name, spec in list(table_specs.items())[:5]:  # Show first 5 tables
                            specs_text += f"\n📋 {table_name.upper()}:\n"
                            for col in spec.columns[:3]:  # Show first 3 columns
                                specs_text += f"  • {col.name}: {col.data_type}"
                                if col.max_length:
                                    specs_text += f"({col.max_length})"
                                if not col.is_nullable:
                                    specs_text += " NOT NULL"
                                if col.is_primary_key:
                                    specs_text += " PRIMARY KEY"
                                specs_text += "\n"
                            if len(spec.columns) > 3:
                                specs_text += f"  ... and {len(spec.columns) - 3} more columns\n"
                        if len(table_specs) > 5:
                            specs_text += f"\n... and {len(table_specs) - 5} more tables\n"
                        self.result_queue.put(("progress", specs_text))
                    
                    # Create specification-driven generator
                    spec_generator = SpecificationDrivenGenerator(self.db_connection, table_specs)
                else:
                    # Use legacy generator
                    generator = DataGenerator(self.schema, config)
                    table_specs = None
                    spec_generator = None
                
                # Apply auto-configuration if enabled
                if hasattr(self, 'auto_config_var') and self.auto_config_var.get():
                    self.result_queue.put(("progress", "🤖 Generating optimal configuration automatically..."))
                    
                    try:
                        from dbmocker.core.smart_generator import create_optimal_generation_config
                        auto_config = create_optimal_generation_config(self.schema, self.db_connection, 10)
                        
                        # Merge auto-config with existing config
                        if auto_config.table_configs:
                            for table_name, table_config in auto_config.table_configs.items():
                                config.table_configs[table_name] = table_config
                            
                            self.result_queue.put(("progress", f"✅ Auto-configuration applied to {len(auto_config.table_configs)} tables"))
                    except Exception as e:
                        self.result_queue.put(("progress", f"⚠️ Auto-configuration failed: {e}"))
                
                # Create inserter based on mode
                if self.spec_driven_var.get() and table_specs:
                    # Create enhanced schema for inserter compatibility
                    from dbmocker.core.models import DatabaseSchema, TableInfo, ColumnInfo
                    
                    # Start with original schema tables
                    enhanced_tables = list(self.schema.tables)
                    existing_table_names = {table.name for table in enhanced_tables}
                    
                    # Add any new tables found by spec analyzer but not in original schema
                    for table_name, spec in table_specs.items():
                        if table_name not in existing_table_names:
                            mock_columns = []
                            for col_spec in spec.columns:
                                mock_columns.append(ColumnInfo(
                                    name=col_spec.name,
                                    data_type=col_spec.base_type.value,
                                    max_length=col_spec.max_length,
                                    is_nullable=col_spec.is_nullable,
                                    is_auto_increment=col_spec.is_auto_increment
                                ))
                            
                            mock_table = TableInfo(
                                name=table_name,
                                columns=mock_columns,
                                row_count=spec.row_count
                            )
                            enhanced_tables.append(mock_table)
                            logger.info(f"Added missing table {table_name} to enhanced schema")
                    
                    enhanced_schema = DatabaseSchema(
                        database_name=self.db_connection.config.database,
                        tables=enhanced_tables
                    )
                    
                    inserter = DataInserter(self.db_connection, enhanced_schema)
                else:
                    # Use legacy mode
                    inserter = DataInserter(self.db_connection, self.schema)
                
                # Collect table configurations from GUI and validate against schema
                table_configs = {}
                schema_table_names = {table.name for table in self.schema.tables}
                skipped_tables = []
                
                for item in self.config_tree.get_children():
                    values = self.config_tree.item(item, "values")
                    table_name = values[0]
                    rows_to_generate = int(values[1])
                    
                    if rows_to_generate > 0:
                        # Only include tables that exist in schema
                        if table_name in schema_table_names:
                            table_configs[table_name] = rows_to_generate
                        else:
                            skipped_tables.append(table_name)
                            self.result_queue.put(("progress", f"⚠️ Skipping table '{table_name}' - not found in current schema analysis"))
                
                if skipped_tables:
                    self.result_queue.put(("progress", f"📋 Skipped {len(skipped_tables)} tables not in schema: {', '.join(skipped_tables)}"))
                
                if not table_configs:
                    self.result_queue.put(("error", "No valid tables selected for generation"))
                    return
                
                self.result_queue.put(("progress", f"📊 Processing {len(table_configs)} valid tables: {', '.join(table_configs.keys())}"))
                
                # If we skipped tables, refresh config tree to remove them
                if skipped_tables:
                    self.result_queue.put(("refresh_config", self.schema))
                
                # Determine processing order
                if self.dependency_aware_var.get() and insertion_plan:
                    # Use dependency-aware batched processing
                    batches = insertion_plan.get_insertion_batches()
                    self.result_queue.put(("progress", f"📦 Processing {len(table_configs)} tables in {len(batches)} dependency batches"))
                    
                    processing_order = []
                    for batch_num, batch in enumerate(batches, 1):
                        batch_tables = [table for table in batch if table in table_configs]
                        if batch_tables:
                            processing_order.append((f"Batch {batch_num}", batch_tables))
                else:
                    # Use GUI order (legacy mode)
                    gui_order = [item[0] for item in [(self.config_tree.item(item, "values")[0], 
                                                     int(self.config_tree.item(item, "values")[1])) 
                                                    for item in self.config_tree.get_children()] 
                               if item[1] > 0 and item[0] in table_configs]
                    processing_order = [("GUI Order", gui_order)]
                    
                # Process tables
                total_generated = 0
                total_inserted = 0
                
                for batch_name, table_list in processing_order:
                    if len(processing_order) > 1:
                        self.result_queue.put(("progress", f"📦 Starting {batch_name} ({len(table_list)} tables)"))
                    
                    for table_name in table_list:
                        rows_to_generate = table_configs[table_name]
                        
                        if self.spec_driven_var.get() and table_specs:
                            # Specification-driven mode
                            if table_name not in table_specs:
                                self.result_queue.put(("progress", f"⚠️ Skipping table '{table_name}' - not found in specification analysis"))
                                continue
                                
                            table_spec = table_specs[table_name]
                            
                            self.result_queue.put(("progress", f"⚡ Generating {rows_to_generate} rows for {table_name} using exact specifications..."))
                            
                            # Generate data using specification-driven approach
                            data = spec_generator._generate_table_data(table_spec, rows_to_generate)
                            total_generated += len(data)
                            
                            # Insert data (if not dry run)
                            if not self.dry_run_var.get():
                                if self.truncate_var.get():
                                    inserter.truncate_table(table_name)
                                
                                # Insert data using table name (not mock table object)
                                rows_inserted = inserter.insert_data(table_name, data, int(self.batch_size_var.get()))
                                total_inserted += rows_inserted.total_rows_generated
                            
                            self.result_queue.put(("table_complete", {
                                'table': table_name,
                                'generated': len(data),
                                'inserted': len(data) if not self.dry_run_var.get() else 0,
                                'spec_driven': True,
                                'batch': batch_name
                            }))
                        else:
                            # Legacy mode - verify table exists in schema
                            schema_table_names = {table.name for table in self.schema.tables}
                            if table_name not in schema_table_names:
                                self.result_queue.put(("progress", f"⚠️ Skipping table '{table_name}' - not found in schema analysis"))
                                continue
                            
                            self.result_queue.put(("progress", f"Generating data for {table_name}..."))
                            
                            # Generate data using legacy approach
                            data = generator.generate_data_for_table(table_name, rows_to_generate)
                            total_generated += len(data)
                            
                            # Insert data (if not dry run)
                            if not self.dry_run_var.get():
                                if self.truncate_var.get():
                                    inserter.truncate_table(table_name)
                                
                                stats = inserter.insert_data(table_name, data, int(self.batch_size_var.get()))
                                total_inserted += stats.total_rows_generated
                            
                            self.result_queue.put(("table_complete", {
                                'table': table_name,
                                'generated': len(data),
                                'inserted': len(data) if not self.dry_run_var.get() else 0,
                                'spec_driven': False,
                                'batch': batch_name
                            }))
                
                # Verify integrity if requested
                if self.verify_var.get() and not self.dry_run_var.get():
                    self.result_queue.put(("progress", "Verifying data integrity..."))
                    integrity_report = inserter.verify_data_integrity()
                    self.result_queue.put(("integrity_check", integrity_report))
                
                self.result_queue.put(("generation_complete", {
                    'total_generated': total_generated,
                    'total_inserted': total_inserted,
                    'spec_driven': self.spec_driven_var.get(),
                    'dependency_aware': self.dependency_aware_var.get(),
                    'tables_analyzed': len(table_specs) if table_specs else 0,
                    'dependency_batches': len(insertion_plan.get_insertion_batches()) if insertion_plan else 0,
                    'tables_processed': len(table_configs)
                }))
                
            except Exception as e:
                self.result_queue.put(("error", str(e)))
        
        # Start generation
        self.generate_button.config(state=tk.DISABLED, text="Generating...")
        self.progress_bar.start()
        
        thread = threading.Thread(target=generation_task)
        thread.daemon = True
        thread.start()
    
    def build_generation_config(self) -> GenerationConfig:
        """Build generation configuration from GUI settings."""
        config = GenerationConfig(
            batch_size=int(self.batch_size_var.get()) if self.batch_size_var.get() else 1000,
            seed=int(self.seed_var.get()) if self.seed_var.get() else None,
            truncate_existing=self.truncate_var.get()
        )
        
        # Add table configurations
        for item in self.config_tree.get_children():
            values = self.config_tree.item(item, "values")
            table_name = values[0]
            rows_to_generate = int(values[1])
            
            config.table_configs[table_name] = TableGenerationConfig(
                rows_to_generate=rows_to_generate
            )
        
        return config
    
    def get_db_config(self) -> DatabaseConfig:
        """Get database configuration from GUI inputs."""
        return DatabaseConfig(
            host=self.host_var.get(),
            port=int(self.port_var.get()),
            database=self.database_var.get(),
            username=self.username_var.get(),
            password=self.password_var.get(),
            driver=self.driver_var.get()
        )
    
    def on_table_select(self, event):
        """Handle table selection in schema tab."""
        # This could be extended to show detailed table information
        pass
    
    def clear_logs(self):
        """Clear the log display."""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)
    
    def save_logs(self):
        """Save logs to a file."""
        filename = filedialog.asksaveasfilename(
            defaultextension=".log",
            filetypes=[("Log files", "*.log"), ("Text files", "*.txt"), ("All files", "*.*")]
        )
        if filename:
            with open(filename, 'w') as f:
                f.write(self.log_text.get(1.0, tk.END))
            messagebox.showinfo("Success", f"Logs saved to {filename}")
    
    def toggle_pattern_options(self):
        """Toggle pattern analysis options based on checkbox state."""
        state = tk.NORMAL if self.analyze_existing_data_var.get() else tk.DISABLED
        self.pattern_sample_entry.config(state=state)
    
    def process_results(self):
        """Process results from background tasks."""
        try:
            while True:
                result_type, data = self.result_queue.get_nowait()
                
                if result_type == "schema_analyzed":
                    self.analyze_button.config(state=tk.NORMAL, text="Connect & Analyze Schema")
                    self.populate_schema_tab(data)
                    self.notebook.select(1)  # Switch to schema tab
                
                elif result_type == "progress":
                    self.progress_label.config(text=data)
                
                elif result_type == "table_complete":
                    message = f"✅ {data['table']}: Generated {data['generated']:,}, Inserted {data['inserted']:,}\n"
                    self.append_to_results(message)
                
                elif result_type == "generation_complete":
                    self.generate_button.config(state=tk.NORMAL, text="🎲 Generate Data")
                    self.progress_bar.stop()
                    self.progress_label.config(text="Generation completed!")
                    
                    summary = f"\n🎉 Generation Summary:\n"
                    summary += f"  Total Generated: {data['total_generated']:,} rows\n"
                    summary += f"  Total Inserted: {data['total_inserted']:,} rows\n"
                    self.append_to_results(summary)
                
                elif result_type == "integrity_check":
                    violations = data.get('foreign_key_violations', []) + data.get('constraint_violations', [])
                    if violations:
                        message = f"\n⚠️  Found {len(violations)} integrity violations\n"
                    else:
                        message = f"\n✅ Data integrity verified\n"
                    self.append_to_results(message)
                
                elif result_type == "refresh_config":
                    # Refresh configuration tab with current schema
                    self.populate_config_tab(data)
                    self.append_to_results("🔄 Configuration refreshed to match current schema\n")
                
                elif result_type == "error":
                    self.analyze_button.config(state=tk.NORMAL, text="Connect & Analyze Schema")
                    self.generate_button.config(state=tk.NORMAL, text="🎲 Generate Data")
                    self.progress_bar.stop()
                    self.progress_label.config(text="Error occurred")
                    messagebox.showerror("Error", data)
        
        except queue.Empty:
            pass
        
        # Schedule next check
        self.root.after(100, self.process_results)
    
    def append_to_results(self, text: str):
        """Append text to results display."""
        self.results_text.config(state=tk.NORMAL)
        self.results_text.insert(tk.END, text)
        self.results_text.see(tk.END)
        self.results_text.config(state=tk.DISABLED)


class GUILogHandler(logging.Handler):
    """Custom log handler for GUI display."""
    
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget
    
    def emit(self, record):
        try:
            msg = self.format(record) + '\n'
            
            def append():
                self.text_widget.config(state=tk.NORMAL)
                self.text_widget.insert(tk.END, msg)
                self.text_widget.see(tk.END)
                self.text_widget.config(state=tk.DISABLED)
            
            # Use after_idle to ensure thread safety
            self.text_widget.after_idle(append)
        except Exception:
            pass


def launch_gui():
    """Launch the JaySoft:DBMocker GUI application."""
    root = tk.Tk()
    app = DBMockerGUI(root)
    root.mainloop()


if __name__ == "__main__":
    launch_gui()
