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
        self.driver_var = tk.StringVar(value="postgresql")
        driver_combo = ttk.Combobox(form_frame, textvariable=self.driver_var, 
                                   values=["postgresql", "mysql", "sqlite"], state="readonly")
        driver_combo.grid(row=0, column=1, sticky=tk.EW, pady=5, padx=(10, 0))
        
        # Host
        ttk.Label(form_frame, text="Host:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.host_var = tk.StringVar(value="localhost")
        ttk.Entry(form_frame, textvariable=self.host_var).grid(row=1, column=1, sticky=tk.EW, pady=5, padx=(10, 0))
        
        # Port
        ttk.Label(form_frame, text="Port:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.port_var = tk.StringVar(value="5432")
        ttk.Entry(form_frame, textvariable=self.port_var).grid(row=2, column=1, sticky=tk.EW, pady=5, padx=(10, 0))
        
        # Database
        ttk.Label(form_frame, text="Database:").grid(row=3, column=0, sticky=tk.W, pady=5)
        self.database_var = tk.StringVar()
        ttk.Entry(form_frame, textvariable=self.database_var).grid(row=3, column=1, sticky=tk.EW, pady=5, padx=(10, 0))
        
        # Username
        ttk.Label(form_frame, text="Username:").grid(row=4, column=0, sticky=tk.W, pady=5)
        self.username_var = tk.StringVar()
        ttk.Entry(form_frame, textvariable=self.username_var).grid(row=4, column=1, sticky=tk.EW, pady=5, padx=(10, 0))
        
        # Password
        ttk.Label(form_frame, text="Password:").grid(row=5, column=0, sticky=tk.W, pady=5)
        self.password_var = tk.StringVar()
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
    
    def start_generation(self):
        """Start data generation process."""
        def generation_task():
            try:
                # Build generation config
                config = self.build_generation_config()
                
                # Initialize generator and inserter
                generator = DataGenerator(self.schema, config)
                inserter = DataInserter(self.db_connection, self.schema)
                
                # Process each table
                total_generated = 0
                total_inserted = 0
                
                for item in self.config_tree.get_children():
                    values = self.config_tree.item(item, "values")
                    table_name = values[0]
                    rows_to_generate = int(values[1])
                    
                    if rows_to_generate <= 0:
                        continue
                    
                    self.result_queue.put(("progress", f"Generating data for {table_name}..."))
                    
                    # Generate data
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
                        'inserted': len(data) if not self.dry_run_var.get() else 0
                    }))
                
                # Verify integrity if requested
                if self.verify_var.get() and not self.dry_run_var.get():
                    self.result_queue.put(("progress", "Verifying data integrity..."))
                    integrity_report = inserter.verify_data_integrity()
                    self.result_queue.put(("integrity_check", integrity_report))
                
                self.result_queue.put(("generation_complete", {
                    'total_generated': total_generated,
                    'total_inserted': total_inserted
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
