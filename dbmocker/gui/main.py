"""
Main GUI application for DBMocker.

NOTE: For enhanced performance features and modern UI, use the Enhanced GUI:
    python run_enhanced_gui.py
    
    Or import from: dbmocker.gui.enhanced_main
    
The enhanced GUI includes:
- Ultra-fast processing for millions of records
- Advanced performance configuration
- Real-time system monitoring
- Modern tabbed interface
- Smart duplicate handling
- Multi-threading and connection pooling
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog, scrolledtext
import threading
import queue
import logging
import platform
import sys
import time
from typing import Optional, Dict, Any
import json
from pathlib import Path
from sqlalchemy import text

from dbmocker.core.database import DatabaseConnection, DatabaseConfig
from dbmocker.core.analyzer import SchemaAnalyzer
from dbmocker.core.generator import DataGenerator
from dbmocker.core.inserter import DataInserter
from dbmocker.core.models import GenerationConfig, TableGenerationConfig, ColumnGenerationConfig
from dbmocker.core.db_spec_analyzer import DatabaseSpecAnalyzer
from dbmocker.core.spec_driven_generator import SpecificationDrivenGenerator
from dbmocker.core.dependency_resolver import DependencyResolver, print_insertion_plan
from dbmocker.core.smart_generator import DependencyAwareGenerator, create_optimal_generation_config

# Set up logger
logger = logging.getLogger(__name__)


class ToolTip:
    """Cross-platform tooltip implementation for GUI elements."""
    
    # Class variable to track the currently active tooltip
    _active_tooltip = None
    
    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self.tooltip_window = None
        self.hover_job = None  # Store the delayed job for cancellation
        self.widget.bind("<Enter>", self.on_enter)
        self.widget.bind("<Leave>", self.on_leave)
        
        # Cross-platform event handling
        if platform.system() == "Darwin":  # macOS
            self.widget.bind("<Button-2>", self.show_tooltip)  # Right click on macOS
        else:  # Windows/Linux
            self.widget.bind("<Button-3>", self.show_tooltip)  # Right click
    
    def on_enter(self, event=None):
        """Show tooltip on mouse enter (with delay)."""
        # Cancel any existing hover job
        if self.hover_job:
            self.widget.after_cancel(self.hover_job)
        
        # Schedule new tooltip show with delay
        self.hover_job = self.widget.after(1000, self.show_tooltip)
    
    def on_leave(self, event=None):
        """Hide tooltip on mouse leave."""
        # Cancel any pending hover job
        if self.hover_job:
            self.widget.after_cancel(self.hover_job)
            self.hover_job = None
        
        # Hide current tooltip
        if self.tooltip_window:
            self.tooltip_window.destroy()
            self.tooltip_window = None
            # Clear active tooltip if it's this one
            if ToolTip._active_tooltip == self:
                ToolTip._active_tooltip = None
    
    def show_tooltip(self, event=None):
        """Display the tooltip."""
        if self.tooltip_window or not self.text:
            return
        
        # Hide any currently active tooltip
        if ToolTip._active_tooltip and ToolTip._active_tooltip != self:
            ToolTip._active_tooltip.hide_tooltip()
        
        # Set this as the active tooltip
        ToolTip._active_tooltip = self
        
        x = self.widget.winfo_rootx() + 25
        y = self.widget.winfo_rooty() + 25
        
        self.tooltip_window = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        
        # Cross-platform styling
        if platform.system() == "Darwin":  # macOS
            tw.configure(bg="systemWindowBackgroundColor")
        else:  # Windows/Linux
            tw.configure(bg="#ffffe0")
        
        label = tk.Label(tw, text=self.text, justify=tk.LEFT,
                        background="#ffffe0" if platform.system() != "Darwin" else "systemWindowBackgroundColor",
                        relief=tk.SOLID, borderwidth=1,
                        font=("Arial", "9", "normal"))
        label.pack(ipadx=1)
    
    def hide_tooltip(self):
        """Hide the tooltip window."""
        if self.tooltip_window:
            self.tooltip_window.destroy()
            self.tooltip_window = None
        
        # Clear active tooltip if it's this one
        if ToolTip._active_tooltip == self:
            ToolTip._active_tooltip = None


class DBMockerGUI:
    """Main GUI application for JaySoft-DBMocker."""
    
    def __init__(self, root: tk.Tk):
        """Initialize the GUI application."""
        self.root = root
        self.root.title("JaySoft-DBMocker - Database Mock Data Generator")
        
        # Cross-platform window configuration
        self.configure_cross_platform_window()
        
        # Set application icon if available
        self.set_application_icon()
        
        # Configure window close behavior
        self.root.protocol("WM_DELETE_WINDOW", self.close_application)
        
        # Add keyboard shortcuts
        self.setup_keyboard_shortcuts()
        
        # Application state
        self.db_connection: Optional[DatabaseConnection] = None
        self.schema = None
        self.generation_config = GenerationConfig()
        
        # Threading
        self.task_queue = queue.Queue()
        self.result_queue = queue.Queue()
        self.stop_generation_flag = threading.Event()  # For stopping generation
        
        # Setup GUI
        self.setup_gui()
        self.setup_logging()
        
        # Start result processing
        self.process_results()
    
    def configure_cross_platform_window(self):
        """Configure window for cross-platform compatibility."""
        system = platform.system()
        
        # Set minimum size for all platforms
        self.root.minsize(900, 600)
        
        if system == "Darwin":  # macOS
            # Start with full screen geometry
            self.root.geometry("1400x900")
            # Enable native macOS window controls
            self.root.tk.call('tk', 'scaling', 1.0)
            # Maximize window on macOS
            try:
                self.root.state('zoomed')
            except tk.TclError:
                # Fallback: get screen dimensions and set to full screen
                screen_width = self.root.winfo_screenwidth()
                screen_height = self.root.winfo_screenheight()
                self.root.geometry(f"{screen_width}x{screen_height}+0+0")
        elif system == "Windows":  # Windows
            # Start maximized/full screen on Windows
            self.root.state('zoomed')
        else:  # Linux
            # Try to maximize on Linux
            try:
                self.root.state('zoomed')
            except tk.TclError:
                # Fallback: get screen dimensions and set to full screen
                try:
                    screen_width = self.root.winfo_screenwidth()
                    screen_height = self.root.winfo_screenheight()
                    self.root.geometry(f"{screen_width}x{screen_height}+0+0")
                except:
                    # Final fallback
                    self.root.geometry("1400x900")
    
    def set_application_icon(self):
        """Set application icon for different platforms."""
        try:
            base_path = Path(__file__).parent.parent.parent / "assets" / "logos"
            system = platform.system()
            
            if system == "Windows":
                # Windows .ico format
                try:
                    icon_path = base_path / "jaysoft_dbmocker_logo.ico"
                    if icon_path.exists():
                        self.root.iconbitmap(str(icon_path))
                except tk.TclError:
                    pass  # Icon file not found, continue without icon
            elif system in ["Linux", "Darwin"]:  # Linux and macOS
                # PNG format for Linux and macOS
                try:
                    icon_path = base_path / "jaysoft_dbmocker_icon.png"
                    if icon_path.exists():
                        icon = tk.PhotoImage(file=str(icon_path))
                        self.root.iconphoto(True, icon)
                except tk.TclError:
                    pass  # Icon file not found, continue without icon
        except Exception:
            pass  # Continue without icon if any error occurs
    
    def setup_keyboard_shortcuts(self):
        """Setup keyboard shortcuts for the application."""
        # F11 for fullscreen toggle
        self.root.bind('<F11>', self.toggle_fullscreen)
        self.root.bind('<Escape>', self.exit_fullscreen)
        
        # Ctrl+Q to quit
        self.root.bind('<Control-q>', lambda e: self.close_application())
        
        # Navigation shortcuts
        self.root.bind('<Control-Left>', lambda e: self.previous_tab())
        self.root.bind('<Control-Right>', lambda e: self.next_tab())
        
        # Focus window (make sure it's in front)
        self.root.focus_force()
        self.root.lift()
    
    def toggle_fullscreen(self, event=None):
        """Toggle fullscreen mode."""
        try:
            current_state = self.root.attributes('-fullscreen')
            self.root.attributes('-fullscreen', not current_state)
        except tk.TclError:
            # Fallback for systems that don't support -fullscreen
            if hasattr(self, '_is_fullscreen') and self._is_fullscreen:
                self.root.state('normal')
                self._is_fullscreen = False
            else:
                self.root.state('zoomed')
                self._is_fullscreen = True
    
    def exit_fullscreen(self, event=None):
        """Exit fullscreen mode."""
        try:
            self.root.attributes('-fullscreen', False)
        except tk.TclError:
            self.root.state('normal')
            self._is_fullscreen = False
    
    def next_tab(self):
        """Navigate to the next tab."""
        current_tab = self.notebook.index(self.notebook.select())
        total_tabs = self.notebook.index("end")
        
        # Validate current tab before moving to next
        if not self.validate_current_tab():
            return
        
        if current_tab < total_tabs - 1:
            self.notebook.select(current_tab + 1)
        self.update_navigation_buttons()
    
    def previous_tab(self):
        """Navigate to the previous tab."""
        current_tab = self.notebook.index(self.notebook.select())
        
        if current_tab > 0:
            self.notebook.select(current_tab - 1)
        self.update_navigation_buttons()
    
    def update_navigation_buttons(self):
        """Update navigation button states based on current tab."""
        current_tab = self.notebook.index(self.notebook.select())
        total_tabs = self.notebook.index("end")
        
        # Enable/disable Previous button
        if current_tab == 0:
            self.prev_button.config(state=tk.DISABLED)
        else:
            self.prev_button.config(state=tk.NORMAL)
        
        # Enable/disable Next button
        if current_tab == total_tabs - 1:
            self.next_button.config(state=tk.DISABLED)
        else:
            self.next_button.config(state=tk.NORMAL)
    
    def setup_gui(self):
        """Setup the GUI layout."""
        # FIRST: Create Done button at bottom to reserve space
        self.setup_done_button()
        
        # THEN: Create main notebook for tabs  
        # Main container with border
        main_container = ttk.Frame(self.root, relief='ridge', borderwidth=3)
        main_container.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        
        self.notebook = ttk.Notebook(main_container)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Bind tab change event to update navigation buttons
        self.notebook.bind("<<NotebookTabChanged>>", lambda e: self.update_navigation_buttons())
        
        # Initialize navigation button states after all tabs are created
        self.root.after(100, self.update_navigation_buttons)
        
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
        main_frame = ttk.Frame(self.connection_frame, relief='solid', borderwidth=1)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)
        
        # Logo and Title
        logo_frame = ttk.LabelFrame(main_frame, text="JaySoft-DBMocker", relief='groove', borderwidth=2)
        logo_frame.pack(fill=tk.X, pady=(0, 15), padx=10)
        
        # Try to load and display logo
        try:
            logo_path = Path(__file__).parent.parent.parent / "assets" / "logos" / "jaysoft_dbmocker_header.png"
            if logo_path.exists():
                from tkinter import PhotoImage
                self.logo_image = PhotoImage(file=str(logo_path))
                logo_label = ttk.Label(logo_frame, image=self.logo_image)
                logo_label.pack(pady=(0, 10))
        except Exception:
            # Fallback to text title if logo loading fails
            pass
        
        # Title
        title_label = ttk.Label(logo_frame, text="Database Connection", 
                               font=("Arial", 16, "bold"))
        title_label.pack()
        
        # Connection form
        form_frame = ttk.LabelFrame(main_frame, text="📊 Connection Details", padding=20, relief='ridge', borderwidth=2)
        form_frame.pack(fill=tk.X, pady=(0, 15), padx=10)
        
        # Database driver
        ttk.Label(form_frame, text="Database Type:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.driver_var = tk.StringVar(value="mysql")
        driver_combo = ttk.Combobox(form_frame, textvariable=self.driver_var, 
                                   values=["postgresql", "mysql", "sqlite"], state="readonly")
        driver_combo.grid(row=0, column=1, sticky=tk.EW, pady=5, padx=(10, 0))
        ToolTip(driver_combo, "Select your database type:\n• MySQL: Default port 3306\n• PostgreSQL: Default port 5432\n• SQLite: File-based database")
        
        # Host
        ttk.Label(form_frame, text="Host:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.host_var = tk.StringVar(value="localhost")
        host_entry = ttk.Entry(form_frame, textvariable=self.host_var)
        host_entry.grid(row=1, column=1, sticky=tk.EW, pady=5, padx=(10, 0))
        ToolTip(host_entry, "Database server hostname or IP address:\n• localhost or 127.0.0.1 for local\n• Remote server IP for network databases")
        
        # Port
        ttk.Label(form_frame, text="Port:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.port_var = tk.StringVar(value="3306")
        port_entry = ttk.Entry(form_frame, textvariable=self.port_var)
        port_entry.grid(row=2, column=1, sticky=tk.EW, pady=5, padx=(10, 0))
        ToolTip(port_entry, "Database server port number:\n• MySQL: 3306 (default)\n• PostgreSQL: 5432 (default)\n• Custom ports as configured")
        

        
        # Database selection
        ttk.Label(form_frame, text="Database:").grid(row=3, column=0, sticky=tk.W, pady=5)
        
        # Database selection frame to hold combobox and refresh button
        db_frame = ttk.Frame(form_frame)
        db_frame.grid(row=3, column=1, sticky=tk.EW, pady=5, padx=(10, 0))
        db_frame.columnconfigure(0, weight=1)
        
        self.database_var = tk.StringVar()
        self.database_combo = ttk.Combobox(db_frame, textvariable=self.database_var, state="readonly")
        self.database_combo.grid(row=0, column=0, sticky=tk.EW, padx=(0, 5))
        ToolTip(self.database_combo, "Select database from server:\n• List populated after successful connection\n• Shows all databases you have access to\n• Examples: gringotts_v2, gringotts_test, etc.")
        
        # Refresh databases button
        self.refresh_db_button = ttk.Button(db_frame, text="🔄", width=3, 
                                           command=self.refresh_databases)
        self.refresh_db_button.grid(row=0, column=1)
        self.refresh_db_button.config(state=tk.DISABLED)  # Initially disabled
        ToolTip(self.refresh_db_button, "Refresh database list:\n• Reconnects to server\n• Updates available databases\n• Use after database changes on server")
        
        # Username
        ttk.Label(form_frame, text="Username:").grid(row=4, column=0, sticky=tk.W, pady=5)
        self.username_var = tk.StringVar(value="root")
        username_entry = ttk.Entry(form_frame, textvariable=self.username_var)
        username_entry.grid(row=4, column=1, sticky=tk.EW, pady=5, padx=(10, 0))
        ToolTip(username_entry, "Database username with sufficient privileges:\n• Read access for schema analysis\n• Write access for data insertion\n• CREATE/DROP for truncation operations")
        
        # Password
        ttk.Label(form_frame, text="Password:").grid(row=5, column=0, sticky=tk.W, pady=5)
        self.password_var = tk.StringVar(value="")
        password_entry = ttk.Entry(form_frame, textvariable=self.password_var, show="*")
        password_entry.grid(row=5, column=1, sticky=tk.EW, pady=5, padx=(10, 0))
        ToolTip(password_entry, "Password for the database user:\n• Stored in memory only\n• Not saved to any files\n• Required for authentication")
        
        form_frame.columnconfigure(1, weight=1)
        
        # Buttons
        button_frame = ttk.LabelFrame(main_frame, text="🔧 Actions", padding=15, relief='ridge', borderwidth=2)
        button_frame.pack(fill=tk.X, padx=10, pady=(10, 0))
        
        self.connect_button = ttk.Button(button_frame, text="🔗 Connect & List Databases", 
                                        command=self.test_connection)
        self.connect_button.pack(side=tk.LEFT)
        ToolTip(self.connect_button, "Connect to server and list databases:\n• Tests connection parameters\n• Validates server accessibility\n• Fetches available databases\n• Populates database selection dropdown")
        
        self.analyze_button = ttk.Button(button_frame, text="Analyze Selected Database", 
                                        command=self.connect_and_analyze, state=tk.DISABLED)
        self.analyze_button.pack(side=tk.LEFT, padx=(10, 0))
        ToolTip(self.analyze_button, "Analyze selected database schema:\n• Connects to selected database\n• Analyzes all tables and relationships\n• Populates schema information for generation")
        
        # Status
        self.connection_status = ttk.Label(main_frame, text="Not connected", foreground="red")
        self.connection_status.pack(pady=(10, 0))
    
    def setup_schema_tab(self):
        """Setup schema analysis tab."""
        main_frame = ttk.Frame(self.schema_frame, relief='solid', borderwidth=1)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)
        
        # Title
        title_label = ttk.Label(main_frame, text="Database Schema", 
                               font=("Arial", 16, "bold"))
        title_label.pack(pady=(0, 20))
        
        # Table list
        table_frame = ttk.LabelFrame(main_frame, text="📋 Database Tables", padding=15, relief='ridge', borderwidth=2)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
        
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
        
        # Scrollbars
        scrollbar = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.table_tree.yview)
        self.table_tree.configure(yscrollcommand=scrollbar.set)
        
        # Pack tree and scrollbar
        self.table_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Add mouse wheel scrolling
        def on_mousewheel(event):
            self.table_tree.yview_scroll(int(-1*(event.delta/120)), "units")
        self.table_tree.bind("<MouseWheel>", on_mousewheel)
        
        # Bind selection event
        self.table_tree.bind("<<TreeviewSelect>>", self.on_table_select)
    
    def setup_config_tab(self):
        """Setup generation configuration tab."""
        main_frame = ttk.Frame(self.config_frame, relief='solid', borderwidth=1)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)
        
        # Title
        title_label = ttk.Label(main_frame, text="Generation Configuration", 
                               font=("Arial", 16, "bold"))
        title_label.pack(pady=(0, 20))
        
        # Global settings with scrollable frame
        global_outer_frame = ttk.LabelFrame(main_frame, text="🌐 Global Settings", padding=5, relief='ridge', borderwidth=2)
        global_outer_frame.pack(fill=tk.X, pady=(0, 15), padx=10)
        
        # Create canvas and scrollbar for global settings
        global_canvas = tk.Canvas(global_outer_frame, height=120)
        global_scrollbar = ttk.Scrollbar(global_outer_frame, orient="vertical", command=global_canvas.yview)
        global_scrollable_frame = ttk.Frame(global_canvas)
        
        global_scrollable_frame.bind(
            "<Configure>",
            lambda e: global_canvas.configure(scrollregion=global_canvas.bbox("all"))
        )
        
        global_canvas.create_window((0, 0), window=global_scrollable_frame, anchor="nw")
        global_canvas.configure(yscrollcommand=global_scrollbar.set)
        
        global_canvas.pack(side="left", fill="both", expand=True)
        global_scrollbar.pack(side="right", fill="y")
        
        # Add touchpad scrolling to global settings canvas
        def on_global_mousewheel(event):
            # Handle both mouse wheel and touchpad scrolling
            if event.delta:
                # Windows/Linux mouse wheel
                global_canvas.yview_scroll(int(-1*(event.delta/120)), "units")
            else:
                # Alternative for some systems
                if event.num == 4:
                    global_canvas.yview_scroll(-1, "units")
                elif event.num == 5:
                    global_canvas.yview_scroll(1, "units")
        
        def on_global_trackpad_scroll(event):
            # Mac touchpad/trackpad scrolling
            global_canvas.yview_scroll(int(-1*event.delta), "units")
        
        # Bind multiple scroll events for cross-platform compatibility
        global_canvas.bind("<MouseWheel>", on_global_mousewheel)  # Windows/Linux
        global_canvas.bind("<Button-4>", on_global_mousewheel)    # Linux scroll up
        global_canvas.bind("<Button-5>", on_global_mousewheel)    # Linux scroll down
        
        # Mac-specific touchpad events
        try:
            global_canvas.bind("<Control-MouseWheel>", on_global_trackpad_scroll)  # Mac trackpad
        except:
            pass
        
        # Use scrollable frame for global settings content
        global_frame = global_scrollable_frame
        
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
        table_config_frame = ttk.LabelFrame(main_frame, text="📋 Table Configuration", padding=20, relief='ridge', borderwidth=2)
        table_config_frame.pack(fill=tk.BOTH, expand=True, padx=10)
        
        # Table selection and row count
        control_frame = ttk.Frame(table_config_frame)
        control_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(control_frame, text="Rows to generate per table:").pack(side=tk.LEFT)
        self.default_rows_var = tk.StringVar(value="1000")
        ttk.Entry(control_frame, textvariable=self.default_rows_var, width=10).pack(side=tk.LEFT, padx=(10, 0))
        
        ttk.Button(control_frame, text="Apply to Selected", 
                  command=self.apply_default_rows).pack(side=tk.RIGHT)
        
        # Bulk selection controls
        bulk_frame = ttk.LabelFrame(table_config_frame, text="🚀 Bulk Operations", padding=10, relief='groove', borderwidth=1)
        bulk_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(bulk_frame, text="Bulk Operations:", font=("Arial", 10, "bold")).pack(side=tk.LEFT)
        
        ttk.Button(bulk_frame, text="📋 Select All for Generation", 
                  command=self.select_all_for_generation).pack(side=tk.LEFT, padx=(10, 5))
        
        ttk.Button(bulk_frame, text="🔄 Select All for Existing", 
                  command=self.select_all_for_existing).pack(side=tk.LEFT, padx=(0, 5))
        
        ttk.Button(bulk_frame, text="❌ Clear All Selections", 
                  command=self.clear_all_selections).pack(side=tk.LEFT, padx=(0, 5))
        
        ttk.Button(bulk_frame, text="🎯 Smart Selection", 
                  command=self.smart_table_selection).pack(side=tk.RIGHT)
        
        # Selection info
        selection_info_frame = ttk.Frame(table_config_frame)
        selection_info_frame.pack(fill=tk.X, pady=(0, 5))
        
        self.selection_info_var = tk.StringVar(value="No tables configured")
        ttk.Label(selection_info_frame, textvariable=self.selection_info_var, 
                 foreground="gray").pack(side=tk.LEFT)
        
        # Create scrollable frame for table tree
        tree_frame = ttk.Frame(table_config_frame)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        
        # Table configuration tree
        config_columns = ("selected", "table", "mode", "duplicate_mode", "rows", "status")
        self.config_tree = ttk.Treeview(tree_frame, columns=config_columns, show="headings", height=10)
        
        self.config_tree.heading("selected", text="☑️ Select")
        self.config_tree.heading("table", text="Table Name")
        self.config_tree.heading("mode", text="Data Mode")
        self.config_tree.heading("duplicate_mode", text="Duplicate Mode")
        self.config_tree.heading("rows", text="Rows to Generate")
        self.config_tree.heading("status", text="Status")
        
        self.config_tree.column("selected", width=70, anchor=tk.CENTER)
        self.config_tree.column("table", width=140)
        self.config_tree.column("mode", width=100)
        self.config_tree.column("duplicate_mode", width=110)
        self.config_tree.column("rows", width=100)
        self.config_tree.column("status", width=100)
        
        # Vertical and horizontal scrollbars
        v_scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.config_tree.yview)
        h_scrollbar = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL, command=self.config_tree.xview)
        
        self.config_tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        # Pack tree and scrollbars using grid for better layout
        self.config_tree.grid(row=0, column=0, sticky="nsew")
        v_scrollbar.grid(row=0, column=1, sticky="ns")
        h_scrollbar.grid(row=1, column=0, sticky="ew")
        
        # Configure grid weights
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)
        
        # Add mouse wheel and touchpad scrolling
        def on_config_mousewheel(event):
            # Handle both mouse wheel and touchpad scrolling
            if event.delta:
                # Windows/Linux mouse wheel
                self.config_tree.yview_scroll(int(-1*(event.delta/120)), "units")
            else:
                # Alternative for some systems
                if event.num == 4:
                    self.config_tree.yview_scroll(-1, "units")
                elif event.num == 5:
                    self.config_tree.yview_scroll(1, "units")
        
        def on_config_trackpad_scroll(event):
            # Mac touchpad/trackpad scrolling
            self.config_tree.yview_scroll(int(-1*event.delta), "units")
        
        # Bind multiple scroll events for cross-platform compatibility
        self.config_tree.bind("<MouseWheel>", on_config_mousewheel)  # Windows/Linux
        self.config_tree.bind("<Button-4>", on_config_mousewheel)    # Linux scroll up
        self.config_tree.bind("<Button-5>", on_config_mousewheel)    # Linux scroll down
        
        # Mac-specific touchpad events
        try:
            self.config_tree.bind("<Control-MouseWheel>", on_config_trackpad_scroll)  # Mac trackpad
        except:
            pass
        
        # Bind clicks for interactions
        self.config_tree.bind("<Double-1>", self.toggle_table_mode)
        self.config_tree.bind("<Button-1>", self.handle_tree_click)
        
        # Add tooltip for duplicate mode column
        ToolTip(self.config_tree, 
               "Duplicate Mode Options:\n"
               "• Generate New: Create unique values (default)\n"
               "• Allow Duplicates: Single value for all rows\n"
               "• Smart Duplicates: Limited set of values with controlled repetition\n\n"
               "Double-click on Data Mode to toggle between Generate New/Use Existing\n"
               "Double-click on Duplicate Mode to cycle through duplicate options")
    
    def setup_generation_tab(self):
        """Setup data generation tab."""
        main_frame = ttk.Frame(self.generation_frame, relief='solid', borderwidth=1)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)
        
        # Title
        title_label = ttk.Label(main_frame, text="Data Generation", 
                               font=("Arial", 16, "bold"))
        title_label.pack(pady=(0, 20))
        
        # Controls
        controls_frame = ttk.LabelFrame(main_frame, text="🎮 Generation Controls", padding=15, relief='ridge', borderwidth=2)
        controls_frame.pack(fill=tk.X, pady=(0, 15), padx=10)
        
        # Generation control buttons
        button_frame = ttk.Frame(controls_frame)
        button_frame.pack(side=tk.LEFT)
        
        self.generate_button = ttk.Button(button_frame, text="🎲 Generate Data", 
                                         command=self.start_generation, state=tk.DISABLED)
        self.generate_button.pack(side=tk.LEFT)
        
        self.stop_button = ttk.Button(button_frame, text="⏹️ Stop Generation", 
                                     command=self.stop_generation, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=(10, 0))
        
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
        
        # Second row of controls for advanced options
        advanced_controls_frame = ttk.Frame(controls_frame)
        advanced_controls_frame.pack(fill=tk.X, pady=(10, 0))
        
        self.fast_generation_var = tk.BooleanVar(value=False)
        fast_gen_cb = ttk.Checkbutton(advanced_controls_frame, text="🚀 Fast Generation (Data Reuse Optimization)", 
                       variable=self.fast_generation_var)
        fast_gen_cb.pack(side=tk.LEFT)
        
        # Add tooltip for fast generation
        ToolTip(fast_gen_cb, 
               "Fast Generation Mode:\n"
               "• Enables smart data reuse for faster generation\n"
               "• Uses duplicate values where constraints allow\n"
               "• Significantly speeds up large dataset generation\n"
               "• Maintains referential integrity and constraints\n"
               "• Recommended for development/testing datasets")
        
        # Progress
        progress_frame = ttk.LabelFrame(main_frame, text="Progress", padding=20)
        progress_frame.pack(fill=tk.X, pady=(0, 20))
        
        self.progress_label = ttk.Label(progress_frame, text="Ready to generate data")
        self.progress_label.pack()
        
        self.progress_bar = ttk.Progressbar(progress_frame, mode='determinate', maximum=100)
        self.progress_bar.pack(fill=tk.X, pady=(10, 0))
        
        # Add percentage label
        self.progress_percentage_label = ttk.Label(progress_frame, text="0%", font=("Arial", 12, "bold"))
        self.progress_percentage_label.pack(pady=(5, 0))
        
        # Results
        results_frame = ttk.LabelFrame(main_frame, text="Generation Results", padding=20)
        results_frame.pack(fill=tk.BOTH, expand=True)
        
        self.results_text = scrolledtext.ScrolledText(results_frame, height=10, state=tk.DISABLED)
        self.results_text.pack(fill=tk.BOTH, expand=True)
        
        # Enhanced mouse wheel scrolling for results
        def on_results_mousewheel(event):
            self.results_text.yview_scroll(int(-1*(event.delta/120)), "units")
        self.results_text.bind("<MouseWheel>", on_results_mousewheel)
    
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
        
        # Enhanced mouse wheel and touchpad scrolling for logs
        def on_logs_mousewheel(event):
            # Handle both mouse wheel and touchpad scrolling
            if event.delta:
                # Windows/Linux mouse wheel
                self.log_text.yview_scroll(int(-1*(event.delta/120)), "units")
            else:
                # Alternative for some systems
                if event.num == 4:
                    self.log_text.yview_scroll(-1, "units")
                elif event.num == 5:
                    self.log_text.yview_scroll(1, "units")
        
        def on_logs_trackpad_scroll(event):
            # Mac touchpad/trackpad scrolling
            self.log_text.yview_scroll(int(-1*event.delta), "units")
        
        # Bind multiple scroll events for cross-platform compatibility
        self.log_text.bind("<MouseWheel>", on_logs_mousewheel)  # Windows/Linux
        self.log_text.bind("<Button-4>", on_logs_mousewheel)    # Linux scroll up
        self.log_text.bind("<Button-5>", on_logs_mousewheel)    # Linux scroll down
        
        # Mac-specific touchpad events
        try:
            self.log_text.bind("<Control-MouseWheel>", on_logs_trackpad_scroll)  # Mac trackpad
        except:
            pass
    
    def setup_logging(self):
        """Setup logging to display in GUI."""
        self.log_handler = GUILogHandler(self.log_text)
        self.log_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', 
                                     datefmt='%H:%M:%S')
        self.log_handler.setFormatter(formatter)
        
        # Add handler to root logger
        logging.getLogger().addHandler(self.log_handler)
    
    def test_server_connection(self):
        """Test database server connection without selecting a specific database."""
        try:
            # Get basic connection config for server connection
            config = self.get_db_config(for_server_connection=True)
            
            # Test server connection
            with DatabaseConnection(config) as db_conn:
                # Just test the connection - don't fetch databases yet
                self.connection_status.config(text="✅ Server connection successful!", foreground="green")
                tk.messagebox.showinfo("Connection Test", "✅ Server connection successful!\n\nYou can now click 'Connect & List Databases' to see available databases.")
                
        except Exception as e:
            self.connection_status.config(text=f"❌ Connection failed: {str(e)}", foreground="red")
            tk.messagebox.showerror("Connection Error", f"❌ Failed to connect to server:\n\n{str(e)}")
    
    def test_connection(self):
        """Test database connection and fetch available databases."""
        try:
            # Get basic connection config without database name
            config = self.get_db_config(for_server_connection=True)
            
            # Test connection and fetch databases
            databases = self.fetch_available_databases(config)
            
            if databases:
                # Update database dropdown
                self.database_combo['values'] = databases
                self.database_combo.config(state="readonly")
                
                # If current database is in the list, select it
                current_db = self.database_var.get()
                if current_db and current_db in databases:
                    self.database_var.set(current_db)
                elif databases:
                    self.database_var.set(databases[0])  # Select first database by default
                
                # Enable controls
                self.connection_status.config(text=f"✅ Connected - {len(databases)} databases found", foreground="green")
                self.analyze_button.config(state=tk.NORMAL)
                self.refresh_db_button.config(state=tk.NORMAL)
                
                messagebox.showinfo("Success", f"Connected successfully!\nFound {len(databases)} databases:\n" + 
                                   "\n".join(databases[:10]) + ("..." if len(databases) > 10 else ""))
            else:
                raise ConnectionError("No databases found or no access granted")
                
        except Exception as e:
            self.connection_status.config(text="❌ Connection failed", foreground="red")
            self.analyze_button.config(state=tk.DISABLED)
            self.refresh_db_button.config(state=tk.DISABLED)
            self.database_combo['values'] = []
            self.database_var.set("")
            messagebox.showerror("Connection Error", str(e))
    
    def connect_and_analyze(self):
        """Connect to database and analyze schema."""
        # Check if a database is selected
        if not self.database_var.get():
            messagebox.showerror("No Database Selected", 
                               "Please select a database from the dropdown first.\n\n" +
                               "If the dropdown is empty, click 'Connect & List Databases' to populate it.")
            return
            
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
        
        # Add tables with default configuration (deselected by default)
        for table in schema.tables:
            self.config_tree.insert("", tk.END, values=(
                "☐",  # Deselected by default - user must choose
                table.name,
                "Generate New",  # Default mode
                "Generate New",  # Default duplicate mode
                self.default_rows_var.get(),
                "Not Selected"  # Clear status for unselected tables
            ))
        
        # Update selection info
        self.update_selection_info()
    
    def apply_default_rows(self):
        """Apply default row count to selected tables only."""
        # Validate that tables are selected first
        if not self.validate_table_selection("apply default row count"):
            return
            
        default_rows = self.default_rows_var.get()
        selected_count = 0
        
        for item in self.config_tree.get_children():
            values = list(self.config_tree.item(item, "values"))
            # Only apply to selected (checked) tables
            if values[0] == "☑️":  # Check if table is selected
                values[4] = default_rows  # Row count is now index 4 (selected, table, mode, duplicate_mode, rows, status)
                self.config_tree.item(item, values=values)
                selected_count += 1
        
        # Update selection info after changes
        self.update_selection_info()
        
        # Show success message
        tk.messagebox.showinfo("Applied", 
                             f"Applied {default_rows} rows to {selected_count} selected table(s).")
    
    def validate_table_selection(self, operation_name="perform this operation"):
        """Validate that at least one table is selected for the operation."""
        selected_count = 0
        
        for item in self.config_tree.get_children():
            values = self.config_tree.item(item, "values")
            if values[0] == "☑️":  # Check if table is selected
                selected_count += 1
        
        if selected_count == 0:
            # Show error message with guidance
            tk.messagebox.showerror(
                "No Tables Selected", 
                f"Please select at least one table before attempting to {operation_name}.\n\n"
                f"How to select tables:\n"
                f"• Click the checkbox (☐) next to table names to select them (☑️)\n"
                f"• Use 'Select All for Generation' to select all tables for new data\n"
                f"• Use 'Select All for Existing' to use existing data from all tables\n"
                f"• Use 'Smart Selection' for intelligent table selection"
            )
            return False
        
        return True
    
    def _get_duplicate_allowed_columns(self, table_name: str) -> list:
        """Get all columns that can safely have duplicate values based on schema constraints."""
        if not hasattr(self, 'schema') or not self.schema:
            return []
        
        table = self.schema.get_table(table_name)
        if not table:
            return []
        
        duplicate_allowed_columns = []
        
        # Get constraint information
        primary_key_columns = table.get_primary_key_columns()
        unique_columns = set()
        auto_increment_columns = set()
        
        # Collect unique constraint columns
        for constraint in table.constraints:
            if constraint.type.value == 'unique':
                unique_columns.update(constraint.columns)
        
        # Collect auto-increment columns
        for column in table.columns:
            if column.is_auto_increment:
                auto_increment_columns.add(column.name)
        
        # Check each column for duplicate eligibility
        for column in table.columns:
            can_have_duplicates = True
            
            # Skip if column is primary key
            if column.name in primary_key_columns:
                can_have_duplicates = False
            
            # Skip if column has unique constraint
            elif column.name in unique_columns:
                can_have_duplicates = False
            
            # Skip if column is auto-increment
            elif column.name in auto_increment_columns:
                can_have_duplicates = False
            
            # Skip if column is part of a composite unique constraint
            else:
                for constraint in table.constraints:
                    if (constraint.type.value == 'unique' and 
                        len(constraint.columns) > 1 and 
                        column.name in constraint.columns):
                        can_have_duplicates = False
                        break
            
            if can_have_duplicates:
                duplicate_allowed_columns.append(column.name)
        
        return duplicate_allowed_columns
    
    def _show_duplicate_columns_info(self, table_name: str, duplicate_mode: str):
        """Show column selection dialog for duplicate mode configuration."""
        duplicate_allowed_columns = self._get_duplicate_allowed_columns(table_name)
        
        if not duplicate_allowed_columns:
            tk.messagebox.showinfo(
                f"Duplicate Mode: {duplicate_mode}",
                f"Table '{table_name}' has no columns that can safely have duplicate values.\n\n"
                f"Reasons columns may be excluded:\n"
                f"• Primary key columns\n"
                f"• Columns with unique constraints\n"
                f"• Auto-increment columns\n"
                f"• Columns in composite unique constraints\n\n"
                f"The duplicate mode will have no effect on this table."
            )
        else:
            # Show column selection dialog
            self._show_duplicate_column_selection_dialog(table_name, duplicate_mode, duplicate_allowed_columns)
    
    def _show_duplicate_column_selection_dialog(self, table_name: str, duplicate_mode: str, allowed_columns: list):
        """Show dialog for selecting which columns should have duplicates."""
        # Create dialog window
        dialog = tk.Toplevel(self.root)
        dialog.title(f"Select Duplicate Columns - {table_name}")
        dialog.geometry("600x500")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Center the dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (dialog.winfo_width() // 2)
        y = (dialog.winfo_screenheight() // 2) - (dialog.winfo_height() // 2)
        dialog.geometry(f"+{x}+{y}")
        
        # Main frame
        main_frame = ttk.Frame(dialog, padding=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Header
        header_frame = ttk.Frame(main_frame)
        header_frame.pack(fill=tk.X, pady=(0, 15))
        
        ttk.Label(header_frame, text=f"🎯 Configure Duplicate Columns", 
                 font=("Arial", 14, "bold")).pack()
        ttk.Label(header_frame, text=f"Table: {table_name} | Mode: {duplicate_mode}",
                 font=("Arial", 10)).pack()
        
        # Description
        mode_descriptions = {
            "Allow Duplicates": "Selected columns will have the same value for all rows",
            "Smart Duplicates": "Selected columns will have limited value sets with controlled probability"
        }
        
        desc_frame = ttk.LabelFrame(main_frame, text="ℹ️ How it works", padding=10)
        desc_frame.pack(fill=tk.X, pady=(0, 15))
        ttk.Label(desc_frame, text=mode_descriptions.get(duplicate_mode, ""),
                 wraplength=500).pack()
        
        # Column selection frame
        selection_frame = ttk.LabelFrame(main_frame, text="📋 Select Columns for Duplicates", padding=10)
        selection_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 15))
        
        # Canvas and scrollbar for column list
        canvas = tk.Canvas(selection_frame, height=200)
        scrollbar = ttk.Scrollbar(selection_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Selection controls
        control_frame = ttk.Frame(selection_frame)
        control_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Button(control_frame, text="✅ Select All", 
                  command=lambda: self._select_all_columns(column_vars, True)).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(control_frame, text="❌ Clear All", 
                  command=lambda: self._select_all_columns(column_vars, False)).pack(side=tk.LEFT)
        
        # Store column variables
        column_vars = {}
        
        # Get table info for column details
        table = self.schema.get_table(table_name) if self.schema else None
        
        # Create checkboxes for each allowed column
        for i, column_name in enumerate(allowed_columns):
            frame = ttk.Frame(scrollable_frame)
            frame.pack(fill=tk.X, pady=2)
            
            # Column checkbox
            var = tk.BooleanVar(value=True)  # Default to selected
            column_vars[column_name] = var
            
            checkbox = ttk.Checkbutton(frame, variable=var, text=column_name)
            checkbox.pack(side=tk.LEFT)
            
            # Column type info
            if table:
                column_info = table.get_column(column_name)
                if column_info:
                    type_text = f"({column_info.data_type.value}"
                    if column_info.max_length:
                        type_text += f", max:{column_info.max_length}"
                    type_text += ")"
                    
                    ttk.Label(frame, text=type_text, foreground="gray").pack(side=tk.LEFT, padx=(10, 0))
        
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Summary
        summary_frame = ttk.Frame(main_frame)
        summary_frame.pack(fill=tk.X, pady=(0, 15))
        
        total_columns = len(self.schema.get_table(table_name).columns) if self.schema and self.schema.get_table(table_name) else len(allowed_columns)
        summary_text = f"📊 {len(allowed_columns)} out of {total_columns} columns can have duplicates (others excluded due to constraints)"
        ttk.Label(summary_frame, text=summary_text, foreground="blue").pack()
        
        # Buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X)
        
        def apply_selection():
            selected_columns = [col for col, var in column_vars.items() if var.get()]
            self._store_duplicate_column_selection(table_name, duplicate_mode, selected_columns)
            dialog.destroy()
        
        def cancel_selection():
            # Reset duplicate mode to Generate New
            for item in self.config_tree.get_children():
                values = list(self.config_tree.item(item, "values"))
                if values[1] == table_name:  # Table name at index 1
                    values[3] = "Generate New"  # Reset duplicate mode
                    self.config_tree.item(item, values=values)
                    break
            dialog.destroy()
        
        ttk.Button(button_frame, text="✅ Apply", command=apply_selection).pack(side=tk.RIGHT, padx=(5, 0))
        ttk.Button(button_frame, text="❌ Cancel", command=cancel_selection).pack(side=tk.RIGHT)
        
        # Focus the dialog
        dialog.focus_set()
    
    def _select_all_columns(self, column_vars: dict, select: bool):
        """Select or deselect all columns in the duplicate column dialog."""
        for var in column_vars.values():
            var.set(select)
    
    def _store_duplicate_column_selection(self, table_name: str, duplicate_mode: str, selected_columns: list):
        """Store the user's duplicate column selection for use during generation."""
        # Initialize storage if needed
        if not hasattr(self, '_duplicate_column_selections'):
            self._duplicate_column_selections = {}
        
        # Store the selection
        self._duplicate_column_selections[table_name] = {
            'mode': duplicate_mode,
            'columns': selected_columns
        }
        
        # Show confirmation
        if selected_columns:
            column_text = ", ".join(selected_columns)
            if len(column_text) > 100:
                column_text = column_text[:100] + "..."
            
            tk.messagebox.showinfo(
                "Duplicate Configuration Applied",
                f"✅ Table '{table_name}' - {duplicate_mode} mode\n\n"
                f"📋 Selected columns for duplicates:\n{column_text}\n\n"
                f"ℹ️ {len(selected_columns)} columns configured for duplicate generation"
            )
        else:
            # No columns selected, revert to Generate New
            for item in self.config_tree.get_children():
                values = list(self.config_tree.item(item, "values"))
                if values[1] == table_name:  # Table name at index 1
                    values[3] = "Generate New"  # Reset duplicate mode
                    self.config_tree.item(item, values=values)
                    break
            
            tk.messagebox.showinfo(
                "No Columns Selected",
                f"No columns were selected for duplicates.\n"
                f"Duplicate mode for '{table_name}' has been reset to 'Generate New'."
            )
    
    def validate_current_tab(self):
        """Validate the current tab before allowing navigation."""
        current_tab = self.notebook.index(self.notebook.select())
        tab_text = self.notebook.tab(current_tab, "text")
        
        # Configuration tab (tab index 1) requires table selection
        if "Configuration" in tab_text:
            return self.validate_table_selection("navigate to the next tab")
        
        # For other tabs, no validation needed
        return True
    
    def handle_tree_click(self, event):
        """Handle clicks on the tree for checkbox and mode toggles."""
        item = self.config_tree.identify_row(event.y)
        if not item:
            return
        
        column = self.config_tree.identify_column(event.x)
        
        # Handle checkbox toggle (selected column)
        if column == "#1":  # Selected column
            values = list(self.config_tree.item(item, "values"))
            current_selection = values[0]
            
            # Toggle checkbox
            if current_selection == "☑️":
                values[0] = "☐"  # Unchecked
                values[5] = "Not Selected"  # Update status (index 5)
            else:
                values[0] = "☑️"  # Checked
                values[5] = "Ready"  # Update status (index 5)
            
            self.config_tree.item(item, values=values)
            self.update_selection_info()
    
    def toggle_table_mode(self, event):
        """Toggle between data modes and duplicate modes for a table."""
        item = self.config_tree.selection()[0] if self.config_tree.selection() else None
        if not item:
            return
        
        # Check which column was clicked
        column = self.config_tree.identify_column(event.x)
        values = list(self.config_tree.item(item, "values"))
        
        if column == "#3":  # Data Mode column (selected, table, mode, duplicate_mode, rows, status)
            current_mode = values[2]  # Mode is at index 2
            
            # Toggle data mode
            if current_mode == "Generate New":
                values[2] = "Use Existing"
                values[4] = "0"  # Set rows to 0 for existing data
            else:
                values[2] = "Generate New"
                values[4] = str(self.default_rows_var.get())  # Reset to default rows
            
            self.config_tree.item(item, values=values)
            self.update_selection_info()
            
        elif column == "#4":  # Duplicate Mode column
            current_duplicate_mode = values[3]  # Duplicate mode is at index 3
            table_name = values[1]  # Table name at index 1
            
            # Cycle through duplicate modes
            if current_duplicate_mode == "Generate New":
                new_mode = "Allow Duplicates"
            elif current_duplicate_mode == "Allow Duplicates":
                new_mode = "Smart Duplicates"
            else:  # Smart Duplicates
                new_mode = "Generate New"
            
            values[3] = new_mode
            self.config_tree.item(item, values=values)
            
            # Show which columns will be affected by duplicate mode
            if new_mode != "Generate New":
                self._show_duplicate_columns_info(table_name, new_mode)
    
    def select_all_for_generation(self):
        """Set all tables to Generate New mode with default rows."""
        default_rows = self.default_rows_var.get()
        for item in self.config_tree.get_children():
            values = list(self.config_tree.item(item, "values"))
            values[0] = "☑️"  # Select checkbox
            values[2] = "Generate New"  # Data Mode
            values[3] = "Generate New"  # Duplicate Mode
            values[4] = default_rows  # Rows
            values[5] = "Ready"  # Status
            self.config_tree.item(item, values=values)
        self.update_selection_info()
    
    def select_all_for_existing(self):
        """Set all tables to Use Existing mode."""
        for item in self.config_tree.get_children():
            values = list(self.config_tree.item(item, "values"))
            values[0] = "☑️"  # Select checkbox
            values[2] = "Use Existing"  # Data Mode
            values[3] = "Generate New"  # Duplicate Mode (irrelevant for existing data)
            values[4] = "0"  # Rows
            values[5] = "Ready"  # Status
            self.config_tree.item(item, values=values)
        self.update_selection_info()
    
    def clear_all_selections(self):
        """Clear all table selections (uncheck all)."""
        for item in self.config_tree.get_children():
            values = list(self.config_tree.item(item, "values"))
            values[0] = "☐"  # Uncheck checkbox
            values[2] = "Generate New"  # Data Mode
            values[3] = "Generate New"  # Duplicate Mode
            values[4] = "0"  # Rows
            values[5] = "Not Selected"  # Status
            self.config_tree.item(item, values=values)
        self.update_selection_info()
    
    def smart_table_selection(self):
        """Apply smart defaults based on table characteristics."""
        if not hasattr(self, 'schema') or not self.schema:
            tk.messagebox.showwarning("No Schema", "Please analyze the database schema first.")
            return
        
        # Get table dependencies and existing data
        dependencies = self.schema.get_table_dependencies()
        
        for item in self.config_tree.get_children():
            values = list(self.config_tree.item(item, "values"))
            table_name = values[1]  # Table name is now at index 1
            
            # Find table info
            table_info = next((t for t in self.schema.tables if t.name == table_name), None)
            if not table_info:
                continue
            
            # Smart selection logic
            values[0] = "☑️"  # Select by default
            
            if table_info.row_count > 0:
                # Table has existing data - suggest using existing for small reference tables
                if table_info.row_count < 100 and not dependencies.get(table_name, []):
                    # Small independent table with existing data - use existing
                    values[2] = "Use Existing"  # Data Mode
                    values[3] = "Generate New"  # Duplicate Mode (irrelevant for existing)
                    values[4] = "0"  # Rows
                else:
                    # Larger table or has dependencies - generate new
                    values[2] = "Generate New"  # Data Mode
                    values[3] = "Generate New"  # Duplicate Mode
                    values[4] = str(min(1000, table_info.row_count * 2))  # 2x existing data
            else:
                # Empty table - generate new data
                values[2] = "Generate New"  # Data Mode
                values[3] = "Generate New"  # Duplicate Mode
                values[4] = "500"  # Default for empty tables
            
            values[5] = "Ready"  # Status
            self.config_tree.item(item, values=values)
        
        self.update_selection_info()
        
        # Analyze FK dependencies for the selection
        self.analyze_and_show_fk_dependencies()
        
        tk.messagebox.showinfo("Smart Selection", 
                              "Applied intelligent defaults based on table characteristics:\n\n"
                              "• Small reference tables (< 100 rows) → Use Existing\n"
                              "• Larger tables with data → Generate New (2x existing)\n"
                              "• Empty tables → Generate New (500 rows)\n\n"
                              "FK dependencies have been analyzed and validated.")
    
    def update_selection_info(self):
        """Update the selection information display."""
        if not hasattr(self, 'config_tree'):
            return
        
        total_tables = len(self.config_tree.get_children())
        selected_count = 0
        generate_count = 0
        existing_count = 0
        total_rows = 0
        
        for item in self.config_tree.get_children():
            values = self.config_tree.item(item, "values")
            selected = values[0] == "☑️"
            mode = values[2]  # Data Mode at index 2
            duplicate_mode = values[3] if len(values) > 3 else "Generate New"  # Duplicate Mode at index 3
            rows = int(values[4]) if len(values) > 4 and values[4].isdigit() else 0  # Rows at index 4
            
            if selected:
                selected_count += 1
                if mode == "Generate New" and rows > 0:
                    generate_count += 1
                    total_rows += rows
                elif mode == "Use Existing":
                    existing_count += 1
        
        info_text = f"Tables: {total_tables} total | {selected_count} selected | {generate_count} generating ({total_rows:,} rows) | {existing_count} using existing"
        self.selection_info_var.set(info_text)
    
    def analyze_and_show_fk_dependencies(self):
        """Analyze and display FK dependencies for current table selection."""
        if not hasattr(self, 'schema') or not self.schema:
            return
        
        # Build temporary config to analyze dependencies
        temp_config = self.build_generation_config()
        
        try:
            # Create temporary generator to analyze dependencies
            from dbmocker.core.smart_generator import DependencyAwareGenerator
            temp_generator = DependencyAwareGenerator(self.schema, temp_config, None)
            
            # Analyze FK dependencies
            fk_dependencies = temp_generator.analyze_fk_dependencies_for_selection()
            
            if fk_dependencies:
                # Update status for tables with FK dependencies to unselected tables
                for item in self.config_tree.get_children():
                    values = list(self.config_tree.item(item, "values"))
                    selected = values[0] == "☑️"  # Check if selected
                    table_name = values[1]  # Table name at index 1
                    mode = values[2]  # Mode at index 2
                    
                    if selected and table_name in fk_dependencies and mode == "Generate New":
                        referenced_tables = fk_dependencies[table_name]
                        values[4] = f"FK→{','.join(referenced_tables[:2])}{'...' if len(referenced_tables) > 2 else ''}"  # Status at index 4
                        self.config_tree.item(item, values=values)
                
                # Show summary in a popup
                dependency_text = "🔗 FK Dependencies Detected:\n\n"
                dependency_text += "Selected tables that will use existing data from unselected tables:\n\n"
                
                for selected_table, referenced_tables in fk_dependencies.items():
                    dependency_text += f"• {selected_table} → {', '.join(referenced_tables)}\n"
                
                dependency_text += "\n✅ These FK relationships will automatically use existing data from the referenced tables."
                
                # Create a more detailed popup
                self.show_fk_dependency_details(fk_dependencies)
            
        except Exception as e:
            logger.debug(f"Could not analyze FK dependencies: {e}")
    
    def show_fk_dependency_details(self, fk_dependencies):
        """Show detailed FK dependency information in a popup window."""
        if not fk_dependencies:
            return
        
        # Create popup window
        popup = tk.Toplevel(self.root)
        popup.title("FK Dependencies Analysis")
        popup.geometry("600x400")
        popup.resizable(True, True)
        popup.transient(self.root)
        popup.grab_set()
        
        # Center the popup
        popup.geometry("+%d+%d" % (self.root.winfo_rootx() + 50, self.root.winfo_rooty() + 50))
        
        # Main frame
        main_frame = ttk.Frame(popup)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Title
        title_label = ttk.Label(main_frame, text="🔗 Foreign Key Dependencies", 
                               font=("Arial", 14, "bold"))
        title_label.pack(pady=(0, 10))
        
        # Description
        desc_text = ("The following selected tables have foreign keys pointing to unselected tables.\n"
                    "The system will automatically use existing data from the unselected tables.")
        desc_label = ttk.Label(main_frame, text=desc_text, wraplength=550)
        desc_label.pack(pady=(0, 15))
        
        # Scrollable text area for dependencies
        text_frame = ttk.Frame(main_frame)
        text_frame.pack(fill=tk.BOTH, expand=True)
        
        text_widget = tk.Text(text_frame, wrap=tk.WORD, height=15, width=70)
        scrollbar = ttk.Scrollbar(text_frame, orient=tk.VERTICAL, command=text_widget.yview)
        text_widget.configure(yscrollcommand=scrollbar.set)
        
        text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Populate dependency information
        for selected_table, referenced_tables in fk_dependencies.items():
            text_widget.insert(tk.END, f"📊 {selected_table} (Selected for Generation)\n", "header")
            text_widget.insert(tk.END, f"   └── Will use existing data from:\n")
            
            for ref_table in referenced_tables:
                # Check if referenced table has data
                table_info = next((t for t in self.schema.tables if t.name == ref_table), None)
                row_count = table_info.row_count if table_info else 0
                status = f"({row_count:,} existing rows)" if row_count > 0 else "(⚠️ No existing data)"
                
                text_widget.insert(tk.END, f"       • {ref_table} {status}\n")
            
            text_widget.insert(tk.END, f"\n")
        
        # Style the header text
        text_widget.tag_configure("header", font=("Arial", 10, "bold"), foreground="blue")
        
        # Make text read-only
        text_widget.config(state=tk.DISABLED)
        
        # Close button
        close_button = ttk.Button(main_frame, text="✅ Understood", command=popup.destroy)
        close_button.pack(pady=(10, 0))
    
    def setup_advanced_tab(self):
        """Setup advanced options tab with all CLI features."""
        main_frame = ttk.Frame(self.advanced_frame, relief='solid', borderwidth=1)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)
        
        # Title
        title_label = ttk.Label(main_frame, text="Advanced Options", 
                               font=("Arial", 16, "bold"))
        title_label.pack(pady=(0, 20))
        
        # Create scrollable frame for all options (full width)
        container_frame = ttk.Frame(main_frame)
        container_frame.pack(fill=tk.BOTH, expand=True)
        
        canvas = tk.Canvas(container_frame)
        scrollbar = ttk.Scrollbar(container_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        # Store the window ID for later configuration
        canvas_window = canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Bind canvas width to scrollable frame width for full width usage
        def configure_scroll_region(event):
            canvas.configure(scrollregion=canvas.bbox("all"))
            # Make scrollable frame use full canvas width
            canvas_width = event.width
            canvas.itemconfig(canvas_window, width=canvas_width)
        
        canvas.bind('<Configure>', configure_scroll_region)
        
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
        pattern_checkbox = ttk.Checkbutton(pattern_analysis_frame, text="🔍 Analyze existing data for realistic patterns", 
                       variable=self.analyze_existing_data_var, command=self.toggle_pattern_options)
        pattern_checkbox.pack(anchor=tk.W, pady=2)
        ToolTip(pattern_checkbox, "Enable intelligent pattern-based generation:\n• Analyzes existing data patterns\n• Generates realistic mock data\n• Maintains domain-specific formats\n• 80% pattern reuse for believable data")
        
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
        
        # Performance Options (NEW)
        performance_frame = ttk.LabelFrame(scrollable_frame, text="⚡ Performance Settings", padding=10)
        performance_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Multi-threading options
        threading_frame = ttk.Frame(performance_frame)
        threading_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(threading_frame, text="Worker Threads:").pack(side=tk.LEFT)
        self.max_workers_var = tk.StringVar(value="4")
        ttk.Entry(threading_frame, textvariable=self.max_workers_var, width=10).pack(side=tk.LEFT, padx=(10, 5))
        ttk.Label(threading_frame, text="(1-16 threads)").pack(side=tk.LEFT)
        
        # Multi-processing options
        multiprocessing_frame = ttk.Frame(performance_frame)
        multiprocessing_frame.pack(fill=tk.X, pady=5)
        
        self.enable_multiprocessing_var = tk.BooleanVar(value=False)
        mp_checkbox = ttk.Checkbutton(multiprocessing_frame, text="🚀 Enable Multiprocessing for Large Datasets", 
                       variable=self.enable_multiprocessing_var, command=self.toggle_multiprocessing_options)
        mp_checkbox.pack(anchor=tk.W, pady=2)
        ToolTip(mp_checkbox, "Enable multiprocessing for millions of records:\n• Dramatically improves performance\n• Uses multiple CPU cores\n• Recommended for >100K rows per table")
        
        # Multiprocessing settings (initially disabled)
        self.mp_settings_frame = ttk.Frame(performance_frame)
        self.mp_settings_frame.pack(fill=tk.X, pady=5)
        
        mp_processes_frame = ttk.Frame(self.mp_settings_frame)
        mp_processes_frame.pack(fill=tk.X, pady=2)
        
        ttk.Label(mp_processes_frame, text="  Max Processes:").pack(side=tk.LEFT)
        self.max_processes_var = tk.StringVar(value="2")
        self.max_processes_entry = ttk.Entry(mp_processes_frame, textvariable=self.max_processes_var, width=10)
        self.max_processes_entry.pack(side=tk.LEFT, padx=(10, 5))
        ttk.Label(mp_processes_frame, text="processes").pack(side=tk.LEFT)
        
        mp_threshold_frame = ttk.Frame(self.mp_settings_frame)
        mp_threshold_frame.pack(fill=tk.X, pady=2)
        
        ttk.Label(mp_threshold_frame, text="  Rows per Process:").pack(side=tk.LEFT)
        self.rows_per_process_var = tk.StringVar(value="100000")
        self.rows_per_process_entry = ttk.Entry(mp_threshold_frame, textvariable=self.rows_per_process_var, width=10)
        self.rows_per_process_entry.pack(side=tk.LEFT, padx=(10, 5))
        ttk.Label(mp_threshold_frame, text="rows threshold").pack(side=tk.LEFT)
        
        # Initially disable multiprocessing settings
        self.toggle_multiprocessing_options()
        
        # Duplicate Options (NEW)
        duplicate_frame = ttk.LabelFrame(scrollable_frame, text="🔄 Duplicate Data Options", padding=10)
        duplicate_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.enable_duplicates_var = tk.BooleanVar(value=False)
        duplicate_checkbox = ttk.Checkbutton(duplicate_frame, text="🔄 Allow Duplicate Values for Specific Columns", 
                       variable=self.enable_duplicates_var, command=self.toggle_duplicate_options)
        duplicate_checkbox.pack(anchor=tk.W, pady=2)
        ToolTip(duplicate_checkbox, "Enable duplicate value generation:\n• Useful for testing duplicate scenarios\n• Applies to non-unique, non-PK columns\n• Generates same value for all rows")
        
        ttk.Label(duplicate_frame, text="Use when testing duplicate handling or need consistent values across rows", 
                 font=("Arial", 9), foreground="gray").pack(anchor=tk.W, pady=(0, 5))
        
        # Duplicate column selection (initially disabled)
        self.duplicate_settings_frame = ttk.Frame(duplicate_frame)
        self.duplicate_settings_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(self.duplicate_settings_frame, text="  Columns for Duplicates:").pack(anchor=tk.W)
        self.duplicate_columns_var = tk.StringVar()
        self.duplicate_columns_entry = ttk.Entry(self.duplicate_settings_frame, textvariable=self.duplicate_columns_var, width=50)
        self.duplicate_columns_entry.pack(fill=tk.X, pady=2)
        ttk.Label(self.duplicate_settings_frame, text="  Format: table.column, table.column (e.g., users.status, orders.priority)", 
                 font=("Arial", 8), foreground="gray").pack(anchor=tk.W)
        
        # Initially disable duplicate settings
        self.toggle_duplicate_options()

        # Advanced Generation Options
        advanced_gen_frame = ttk.LabelFrame(scrollable_frame, text="🚀 Advanced Generation Modes", padding=10)
        advanced_gen_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Generation Mode Selection
        mode_frame = ttk.Frame(advanced_gen_frame)
        mode_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(mode_frame, text="Generation Mode:", font=("Arial", 10, "bold")).pack(anchor=tk.W)
        
        self.generation_mode_var = tk.StringVar(value="standard")
        ttk.Radiobutton(mode_frame, text="🔄 Standard Generation", 
                       variable=self.generation_mode_var, value="standard").pack(anchor=tk.W, pady=2)
        ttk.Radiobutton(mode_frame, text="🧠 Smart Dependency-Aware Generation", 
                       variable=self.generation_mode_var, value="smart").pack(anchor=tk.W, pady=2)
        ttk.Radiobutton(mode_frame, text="🔍 Specification-Driven Generation (DESCRIBE-based)", 
                       variable=self.generation_mode_var, value="spec").pack(anchor=tk.W, pady=2)
        
        # Mode descriptions
        desc_frame = ttk.Frame(advanced_gen_frame)
        desc_frame.pack(fill=tk.X, pady=(0, 10))
        
        mode_descriptions = {
            "standard": "Uses pattern analysis and standard constraint handling",
            "smart": "Analyzes table dependencies for optimal insertion order and FK handling", 
            "spec": "Uses exact DESCRIBE output for precise type and constraint compliance"
        }
        
        self.mode_desc_var = tk.StringVar(value=mode_descriptions["standard"])
        ttk.Label(desc_frame, textvariable=self.mode_desc_var, 
                 font=("Arial", 9), foreground="gray").pack(anchor=tk.W)
        
        # Bind mode selection to update description
        def update_mode_description(*args):
            mode = self.generation_mode_var.get()
            self.mode_desc_var.set(mode_descriptions.get(mode, ""))
        
        self.generation_mode_var.trace('w', update_mode_description)
        
        # Advanced Options
        self.show_dependency_plan_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(advanced_gen_frame, text="📋 Show dependency insertion plan", 
                       variable=self.show_dependency_plan_var).pack(anchor=tk.W, pady=2)
        
        self.show_table_specs_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(advanced_gen_frame, text="📊 Show detailed table specifications", 
                       variable=self.show_table_specs_var).pack(anchor=tk.W, pady=2)
        
        # Max tables to show for specs
        spec_limit_frame = ttk.Frame(advanced_gen_frame)
        spec_limit_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(spec_limit_frame, text="Max tables to show in specs:").pack(side=tk.LEFT)
        self.max_tables_shown_var = tk.StringVar(value="5")
        ttk.Entry(spec_limit_frame, textvariable=self.max_tables_shown_var, width=10).pack(side=tk.LEFT, padx=(10, 5))
        ttk.Label(spec_limit_frame, text="tables").pack(side=tk.LEFT)
        
        # Pack canvas and scrollbar for full width
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Bind mousewheel and touchpad scrolling to canvas
        def _on_mousewheel(event):
            # Handle both mouse wheel and touchpad scrolling
            if event.delta:
                # Windows/Linux mouse wheel
                canvas.yview_scroll(int(-1*(event.delta/120)), "units")
            else:
                # Alternative for some systems
                if event.num == 4:
                    canvas.yview_scroll(-1, "units")
                elif event.num == 5:
                    canvas.yview_scroll(1, "units")
        
        def _on_trackpad_scroll(event):
            # Mac touchpad/trackpad scrolling
            canvas.yview_scroll(int(-1*event.delta), "units")
        
        # Bind multiple scroll events for cross-platform compatibility
        canvas.bind_all("<MouseWheel>", _on_mousewheel)  # Windows/Linux
        canvas.bind_all("<Button-4>", _on_mousewheel)    # Linux scroll up
        canvas.bind_all("<Button-5>", _on_mousewheel)    # Linux scroll down
        
        # Mac-specific touchpad events
        try:
            canvas.bind_all("<Control-MouseWheel>", _on_trackpad_scroll)  # Mac trackpad
        except:
            pass
        
        # Focus the canvas for scroll events
        canvas.focus_set()
    
    def toggle_multiprocessing_options(self):
        """Toggle multiprocessing settings visibility."""
        if self.enable_multiprocessing_var.get():
            # Enable multiprocessing options
            for child in self.mp_settings_frame.winfo_children():
                for subchild in child.winfo_children():
                    if isinstance(subchild, ttk.Entry):
                        subchild.config(state=tk.NORMAL)
        else:
            # Disable multiprocessing options
            for child in self.mp_settings_frame.winfo_children():
                for subchild in child.winfo_children():
                    if isinstance(subchild, ttk.Entry):
                        subchild.config(state=tk.DISABLED)
    
    def toggle_duplicate_options(self):
        """Toggle duplicate settings visibility."""
        if self.enable_duplicates_var.get():
            # Enable duplicate options
            self.duplicate_columns_entry.config(state=tk.NORMAL)
        else:
            # Disable duplicate options
            self.duplicate_columns_entry.config(state=tk.DISABLED)
    
    def setup_done_button(self):
        """Setup the Done button at the bottom of the window."""
        # Create a frame for the bottom buttons - ensure it's at the very bottom
        bottom_frame = ttk.LabelFrame(self.root, text="🔧 Actions", padding=10, relief='ridge', borderwidth=2)
        bottom_frame.pack(side=tk.BOTTOM, fill=tk.X, padx=8, pady=8)
        
        # Add separator line
        separator = ttk.Separator(bottom_frame, orient='horizontal')
        separator.pack(fill=tk.X, pady=(0, 10))
        
        # Navigation frame for tab controls
        nav_frame = ttk.Frame(bottom_frame)
        nav_frame.pack(side=tk.LEFT)
        
        # Previous button
        self.prev_button = ttk.Button(nav_frame, text="⬅️ Previous", 
                                     command=self.previous_tab)
        self.prev_button.pack(side=tk.LEFT, padx=(0, 5))
        ToolTip(self.prev_button, "Go to previous tab:\n• Navigate backward through workflow\n• Quick tab switching\n• Keyboard: Ctrl+Left")
        
        # Next button
        self.next_button = ttk.Button(nav_frame, text="Next ➡️", 
                                     command=self.next_tab)
        self.next_button.pack(side=tk.LEFT, padx=(0, 10))
        ToolTip(self.next_button, "Go to next tab:\n• Navigate forward through workflow\n• Quick tab switching\n• Keyboard: Ctrl+Right")
        
        # Button frame for proper alignment
        button_frame = ttk.Frame(bottom_frame)
        button_frame.pack(side=tk.RIGHT)
        
        # Done button
        self.done_button = ttk.Button(button_frame, text="✅ Done", 
                                     command=self.close_application, 
                                     style="Accent.TButton")
        self.done_button.pack(side=tk.RIGHT, padx=(0, 5))
        
        # Add tooltip
        ToolTip(self.done_button, "Close JaySoft-DBMocker application:\n• Safely terminates all connections\n• Stops any running operations\n• Exits the application")
        
        # Add about button for additional info
        self.about_button = ttk.Button(button_frame, text="ℹ️ About", 
                                      command=self.show_about_dialog)
        self.about_button.pack(side=tk.RIGHT, padx=(0, 10))
        
        ToolTip(self.about_button, "Show application information:\n• Version details\n• Platform information\n• Credits and support")
    
    def close_application(self):
        """Close the application gracefully."""
        try:
            # Ask for confirmation if operations are running
            if hasattr(self, 'generation_thread') and self.generation_thread and self.generation_thread.is_alive():
                result = messagebox.askyesno(
                    "Confirm Exit", 
                    "Data generation is currently running.\n\nAre you sure you want to exit?\nThis will stop the current operation.",
                    icon='warning'
                )
                if not result:
                    return
            
            # Close database connection if exists
            if hasattr(self, 'db_connection') and self.db_connection:
                try:
                    self.db_connection.close()
                except Exception as e:
                    pass  # Continue closing even if connection close fails
            
            # Destroy the main window
            self.root.quit()
            self.root.destroy()
            
        except Exception as e:
            # Force close even if there are errors
            try:
                self.root.quit()
            except:
                pass
            try:
                self.root.destroy()
            except:
                pass
    
    def show_about_dialog(self):
        """Show application about dialog."""
        # Create custom about dialog with logo
        about_window = tk.Toplevel(self.root)
        about_window.title("About JaySoft-DBMocker")
        about_window.geometry("500x600")
        about_window.resizable(False, False)
        about_window.transient(self.root)
        about_window.grab_set()
        
        # Center the dialog
        about_window.update_idletasks()
        x = (about_window.winfo_screenwidth() // 2) - (500 // 2)
        y = (about_window.winfo_screenheight() // 2) - (600 // 2)
        about_window.geometry(f"500x600+{x}+{y}")
        
        # Main frame
        main_frame = ttk.Frame(about_window)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Logo
        try:
            logo_path = Path(__file__).parent.parent.parent / "assets" / "logos" / "jaysoft_dbmocker_logo.png"
            if logo_path.exists():
                about_logo = tk.PhotoImage(file=str(logo_path))
                logo_label = ttk.Label(main_frame, image=about_logo)
                logo_label.image = about_logo  # Keep a reference
                logo_label.pack(pady=(0, 20))
        except Exception:
            # Fallback to text if logo fails
            title_label = ttk.Label(main_frame, text="JaySoft-DBMocker", 
                                   font=("Arial", 24, "bold"))
            title_label.pack(pady=(0, 20))
        
        # About text
        about_text = f'''Database Mock Data Generator

Version: 2.0.0
Platform: {platform.system()} {platform.release()}
Python: {platform.python_version()}
Architecture: {platform.machine()}

🎯 Features:
• Pattern-based realistic data generation
• Smart dependency-aware insertion
• Specification-driven generation
• Cross-platform GUI and CLI support
• Advanced constraint handling

👨‍💻 Developed by: JaySoft Development
📧 Support: Contact through GitHub repository
🌐 Repository: github.com/iamjpsonkar/JaySoft-DBMocker

Enterprise-grade mock data generation for professional development.'''
        
        text_label = ttk.Label(main_frame, text=about_text, justify=tk.CENTER)
        text_label.pack(pady=(0, 20))
        
        # Close button
        close_button = ttk.Button(main_frame, text="Close", 
                                 command=about_window.destroy)
        close_button.pack(pady=10)
    
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
                table_name = values[1]  # Table name is now at index 1
                
                if table_name in table_configs:
                    table_config = table_configs[table_name]
                    values[0] = "☑️"  # Select the table
                    
                    if 'use_existing_data' in table_config and table_config['use_existing_data']:
                        values[2] = "Use Existing"  # Mode at index 2
                        values[4] = "0"  # Rows at index 4
                    elif 'rows_to_generate' in table_config:
                        values[2] = "Generate New"  # Mode at index 2
                        values[4] = table_config['rows_to_generate']  # Rows at index 4
                    
                    values[5] = "Ready"  # Status at index 5
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
                selected = values[0] == "☑️"  # Check if selected
                table_name = values[1]  # Table name at index 1
                mode = values[2]  # Mode at index 2
                rows_to_generate = int(values[4]) if len(values) > 4 and values[4].isdigit() else 0  # Rows at index 4
                
                if selected and rows_to_generate > 0:
                    config['table_configs'][table_name] = {
                        'rows_to_generate': rows_to_generate,
                        'use_existing_data': mode == "Use Existing"
                    }
        
        return config
    

    
    def start_generation(self):
        """Start data generation process."""
        # Validate table selection before starting generation
        if not self.validate_table_selection("start data generation"):
            return
            
        def generation_task():
            try:
                start_time = time.time()  # Track generation duration
                thread_id = threading.current_thread().ident
                logger.info(f"🧵 Generation thread started (Thread ID: {thread_id})")
                logger.info(f"⏱️ Generation start time: {time.strftime('%H:%M:%S', time.localtime(start_time))}")
                
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
                    # Use enhanced generator if parallel processing is enabled
                    if use_parallel:
                        from dbmocker.core.parallel_generator import ParallelDataGenerator
                        generator = ParallelDataGenerator(self.schema, config, self.db_connection)
                        self.result_queue.put(("progress", "🚀 Using parallel data generator"))
                    else:
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
                
                # Determine if we should use parallel processing
                use_parallel = config.enable_multiprocessing or config.max_workers > 1
                
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
                    
                    if use_parallel:
                        from dbmocker.core.parallel_generator import ParallelDataInserter
                        inserter = ParallelDataInserter(self.db_connection, enhanced_schema)
                        self.result_queue.put(("progress", "🚀 Using parallel inserter with enhanced schema"))
                    else:
                        inserter = DataInserter(self.db_connection, enhanced_schema)
                else:
                    # Use legacy mode
                    if use_parallel:
                        from dbmocker.core.parallel_generator import ParallelDataInserter
                        inserter = ParallelDataInserter(self.db_connection, self.schema)
                        self.result_queue.put(("progress", "🚀 Using parallel inserter"))
                    else:
                        inserter = DataInserter(self.db_connection, self.schema)
                
                # Collect table configurations from GUI and validate against schema
                table_configs = {}
                use_existing_tables = []
                schema_table_names = {table.name for table in self.schema.tables}
                skipped_tables = []
                
                for item in self.config_tree.get_children():
                    values = self.config_tree.item(item, "values")
                    selected = values[0] == "☑️"  # Check if selected
                    table_name = values[1]  # Table name at index 1
                    mode = values[2]  # Mode at index 2
                    rows_to_generate = int(values[4]) if len(values) > 4 and values[4].isdigit() else 0  # Rows at index 4
                    
                    # Only include selected tables that exist in schema
                    if selected and table_name in schema_table_names:
                        if mode == "Use Existing":
                            use_existing_tables.append(table_name)
                            # Don't add to table_configs as we're using existing data
                        elif rows_to_generate > 0:
                            table_configs[table_name] = rows_to_generate
                    else:
                        skipped_tables.append(table_name)
                        self.result_queue.put(("progress", f"⚠️ Skipping table '{table_name}' - not found in current schema analysis"))
                
                if skipped_tables:
                    self.result_queue.put(("progress", f"📋 Skipped {len(skipped_tables)} tables not in schema: {', '.join(skipped_tables)}"))
                
                if not table_configs and not use_existing_tables:
                    self.result_queue.put(("error", "No valid tables selected for generation or using existing data"))
                    return
                
                # Build status message
                status_parts = []
                if table_configs:
                    status_parts.append(f"{len(table_configs)} tables generating new data: {', '.join(table_configs.keys())}")
                if use_existing_tables:
                    status_parts.append(f"{len(use_existing_tables)} tables using existing data: {', '.join(use_existing_tables)}")
                
                self.result_queue.put(("progress", f"📊 Processing {' | '.join(status_parts)}"))
                
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
                    gui_order = [item[0] for item in [(self.config_tree.item(item, "values")[1], 
                                                     int(self.config_tree.item(item, "values")[3]) if self.config_tree.item(item, "values")[3].isdigit() else 0) 
                                                    for item in self.config_tree.get_children()
                                                    if self.config_tree.item(item, "values")[0] == "☑️"] 
                               if item[1] > 0 and item[0] in table_configs]
                    processing_order = [("GUI Order", gui_order)]
                    
                # Process tables with progress tracking
                total_generated = 0
                total_inserted = 0
                
                # Calculate total work for progress tracking
                total_rows_to_generate = sum(table_configs.values())
                completed_rows = 0
                
                # Check if fast generation mode is enabled (optimization)
                fast_generation_enabled = hasattr(self, 'fast_generation_var') and self.fast_generation_var.get()
                if fast_generation_enabled:
                    logger.info(f"🚀 Fast Generation Mode enabled - optimizing data reuse")
                    self.result_queue.put(("progress", "🚀 Fast Generation Mode: Using data reuse optimization"))
                
                logger.info(f"📊 Total rows to generate: {total_rows_to_generate:,} across {len(table_configs)} tables")
                logger.info(f"🔧 Processing mode: {'Parallel' if use_parallel else 'Sequential'}")
                logger.info(f"📦 Dependency-aware: {self.dependency_aware_var.get()}")
                logger.info(f"🔍 Spec-driven: {self.spec_driven_var.get()}")
                
                for batch_name, table_list in processing_order:
                    # Check for stop signal
                    if self.stop_generation_flag.is_set():
                        logger.info("🛑 Generation stopped by user request")
                        self.result_queue.put(("generation_stopped", {
                            'total_generated': total_generated,
                            'total_inserted': total_inserted,
                            'completed_tables': completed_rows,
                            'message': 'Generation stopped by user'
                        }))
                        return
                    
                    if len(processing_order) > 1:
                        self.result_queue.put(("progress", f"📦 Starting {batch_name} ({len(table_list)} tables)"))
                    
                    for table_name in table_list:
                        # Check for stop signal before each table
                        if self.stop_generation_flag.is_set():
                            logger.info(f"🛑 Generation stopped before processing table {table_name}")
                            self.result_queue.put(("generation_stopped", {
                                'total_generated': total_generated,
                                'total_inserted': total_inserted,
                                'completed_tables': completed_rows,
                                'last_table': table_name,
                                'message': f'Generation stopped before processing {table_name}'
                            }))
                            return
                        rows_to_generate = table_configs[table_name]
                        table_start_time = time.time()
                        
                        # Calculate and report progress percentage
                        progress_percentage = (completed_rows / total_rows_to_generate) * 100 if total_rows_to_generate > 0 else 0
                        logger.info(f"📊 Table {table_name}: Starting generation of {rows_to_generate:,} rows (Progress: {progress_percentage:.1f}%)")
                        self.result_queue.put(("progress", f"📊 Progress: {progress_percentage:.1f}% - Processing {table_name} ({rows_to_generate:,} rows)"))
                        
                        if self.spec_driven_var.get() and table_specs:
                            # Specification-driven mode
                            if table_name not in table_specs:
                                self.result_queue.put(("progress", f"⚠️ Skipping table '{table_name}' - not found in specification analysis"))
                                continue
                                
                            table_spec = table_specs[table_name]
                            
                            self.result_queue.put(("progress", f"⚡ Generating {rows_to_generate:,} rows for {table_name} using exact specifications..."))
                            
                            # Apply fast generation optimization if enabled
                            if fast_generation_enabled:
                                logger.info(f"🏎️ Table {table_name}: Applying fast generation optimization")
                                self.result_queue.put(("progress", f"🏎️ Fast mode: Optimizing data generation for {table_name}"))
                                # Enable duplicate reuse in spec generator for faster generation
                                original_duplicate_mode = getattr(table_spec, 'duplicate_allowed', False)
                                if hasattr(table_spec, 'duplicate_allowed'):
                                    table_spec.duplicate_allowed = True
                                    logger.debug(f"🔧 Table {table_name}: Enabled duplicate mode for spec generator")
                            
                            # Generate data using specification-driven approach with timeout handling
                            try:
                                # Add progress tracking for large operations
                                if rows_to_generate > 50000:
                                    logger.info(f"⏳ Large dataset detected for {table_name}, monitoring progress...")
                                
                                # Pass stop flag to generator if supported
                                if hasattr(spec_generator, '_generate_table_data_with_stop'):
                                    data = spec_generator._generate_table_data_with_stop(table_spec, rows_to_generate, self.stop_generation_flag)
                                else:
                                    data = spec_generator._generate_table_data(table_spec, rows_to_generate)
                                
                                # Check if generation was successful
                                if not data:
                                    logger.warning(f"⚠️ No data generated for table {table_name}")
                                    continue
                                    
                                logger.info(f"✅ Successfully generated {len(data):,} rows for {table_name}")
                                
                            except Exception as e:
                                logger.error(f"❌ Error generating data for table {table_name}: {str(e)}")
                                self.result_queue.put(("progress", f"❌ Error generating {table_name}: {str(e)}"))
                                continue
                            
                            # Restore original setting
                            if fast_generation_enabled and hasattr(table_spec, 'duplicate_allowed'):
                                table_spec.duplicate_allowed = original_duplicate_mode
                            total_generated += len(data)
                            
                            # Insert data (if not dry run)
                            if not self.dry_run_var.get():
                                if self.truncate_var.get():
                                    inserter.truncate_table(table_name)
                                
                                # Use parallel insertion if available
                                if hasattr(inserter, 'insert_data_parallel') and use_parallel:
                                    rows_inserted = inserter.insert_data_parallel(
                                        table_name, data, int(self.batch_size_var.get()), config.max_workers
                                    )
                                else:
                                    rows_inserted = inserter.insert_data(table_name, data, int(self.batch_size_var.get()))
                                total_inserted += rows_inserted.total_rows_generated
                            
                            # Calculate table generation performance
                            table_end_time = time.time()
                            table_duration = table_end_time - table_start_time
                            rows_per_second = len(data) / table_duration if table_duration > 0 else 0
                            
                            # Update progress tracking
                            completed_rows += len(data)
                            final_progress = (completed_rows / total_rows_to_generate) * 100 if total_rows_to_generate > 0 else 100
                            
                            logger.info(f"✅ Table {table_name}: Generated {len(data):,} rows in {table_duration:.2f}s ({rows_per_second:,.0f} rows/sec)")
                            
                            self.result_queue.put(("table_complete", {
                                'table': table_name,
                                'generated': len(data),
                                'inserted': len(data) if not self.dry_run_var.get() else 0,
                                'spec_driven': True,
                                'batch': batch_name,
                                'progress_percentage': final_progress,
                                'completed_rows': completed_rows,
                                'total_rows': total_rows_to_generate,
                                'duration': table_duration,
                                'rows_per_second': rows_per_second
                            }))
                        else:
                            # Legacy mode - verify table exists in schema
                            schema_table_names = {table.name for table in self.schema.tables}
                            if table_name not in schema_table_names:
                                self.result_queue.put(("progress", f"⚠️ Skipping table '{table_name}' - not found in schema analysis"))
                                continue
                            
                            self.result_queue.put(("progress", f"⚡ Generating {rows_to_generate:,} rows for {table_name}..."))
                            
                            # Apply fast generation optimization if enabled
                            if fast_generation_enabled:
                                logger.info(f"🏎️ Table {table_name}: Applying fast generation optimization (Legacy Mode)")
                                self.result_queue.put(("progress", f"🏎️ Fast mode: Enabling data reuse optimization for {table_name}"))
                                # Enable duplicate mode for faster generation
                                original_duplicate_allowed = config.duplicate_allowed
                                original_global_mode = config.global_duplicate_mode
                                config.duplicate_allowed = True
                                config.global_duplicate_mode = "smart_duplicates"
                                logger.debug(f"🔧 Table {table_name}: Updated global config - duplicate_allowed: True, mode: smart_duplicates")
                                
                                # Apply duplicate configuration to all safe columns
                                if table_name in config.table_configs:
                                    table_config = config.table_configs[table_name]
                                else:
                                    from dbmocker.core.models import TableGenerationConfig
                                    table_config = TableGenerationConfig()
                                    config.table_configs[table_name] = table_config
                                    
                                # Get columns that can safely have duplicates
                                duplicate_allowed_columns = self._get_duplicate_allowed_columns(table_name)
                                logger.info(f"🎯 Table {table_name}: Found {len(duplicate_allowed_columns)} columns eligible for fast mode")
                                for column_name in duplicate_allowed_columns:
                                    from dbmocker.core.models import ColumnGenerationConfig
                                    table_config.column_configs[column_name] = ColumnGenerationConfig(
                                        duplicate_mode="smart_duplicates",
                                        duplicate_probability=0.7,  # High probability for speed
                                        max_duplicate_values=5      # Limited set for fast reuse
                                    )
                                    logger.debug(f"🔧 Column {table_name}.{column_name}: Configured for smart duplicates (prob=0.7, max_values=5)")
                                
                                self.result_queue.put(("progress", f"🎯 Fast mode: Applied to {len(duplicate_allowed_columns)} columns in {table_name}"))
                            
                            # Generate data using enhanced generator if available with error handling
                            try:
                                # Add progress tracking for large operations
                                if rows_to_generate > 50000:
                                    logger.info(f"⏳ Large dataset detected for {table_name}, monitoring progress...")
                                
                                if hasattr(generator, 'generate_data_for_table_parallel') and use_parallel:
                                    logger.info(f"🔄 Table {table_name}: Using parallel generation (workers: {config.max_workers})")
                                    # Pass stop flag to parallel generator if supported
                                    if hasattr(generator, 'set_stop_flag'):
                                        generator.set_stop_flag(self.stop_generation_flag)
                                    data = generator.generate_data_for_table_parallel(table_name, rows_to_generate)
                                else:
                                    logger.info(f"🔄 Table {table_name}: Using sequential generation")
                                    # Pass stop flag to sequential generator if supported
                                    if hasattr(generator, 'set_stop_flag'):
                                        generator.set_stop_flag(self.stop_generation_flag)
                                    data = generator.generate_data_for_table(table_name, rows_to_generate)
                                
                                # Check if generation was successful
                                if not data:
                                    logger.warning(f"⚠️ No data generated for table {table_name}")
                                    continue
                                    
                                logger.info(f"✅ Successfully generated {len(data):,} rows for {table_name}")
                                
                            except Exception as e:
                                logger.error(f"❌ Error generating data for table {table_name}: {str(e)}")
                                self.result_queue.put(("progress", f"❌ Error generating {table_name}: {str(e)}"))
                                # Restore settings if fast mode was used
                                if fast_generation_enabled:
                                    config.duplicate_allowed = original_duplicate_allowed
                                    config.global_duplicate_mode = original_global_mode
                                continue
                                
                            # Restore original settings if fast mode was used
                            if fast_generation_enabled:
                                config.duplicate_allowed = original_duplicate_allowed
                                config.global_duplicate_mode = original_global_mode
                            total_generated += len(data)
                            
                            # Insert data (if not dry run)
                            if not self.dry_run_var.get():
                                if self.truncate_var.get():
                                    inserter.truncate_table(table_name)
                                
                                # Use parallel insertion if available
                                if hasattr(inserter, 'insert_data_parallel') and use_parallel:
                                    stats = inserter.insert_data_parallel(
                                        table_name, data, int(self.batch_size_var.get()), config.max_workers
                                    )
                                else:
                                    stats = inserter.insert_data(table_name, data, int(self.batch_size_var.get()))
                                total_inserted += stats.total_rows_generated
                            
                            # Calculate table generation performance
                            table_end_time = time.time()
                            table_duration = table_end_time - table_start_time
                            rows_per_second = len(data) / table_duration if table_duration > 0 else 0
                            
                            # Update progress tracking
                            completed_rows += len(data)
                            final_progress = (completed_rows / total_rows_to_generate) * 100 if total_rows_to_generate > 0 else 100
                            
                            mode_indicator = "🚀" if fast_generation_enabled else "🔄"
                            parallel_indicator = "Parallel" if use_parallel else "Sequential"
                            logger.info(f"✅ Table {table_name}: Generated {len(data):,} rows in {table_duration:.2f}s ({rows_per_second:,.0f} rows/sec) [{mode_indicator} {parallel_indicator}]")
                            
                            self.result_queue.put(("table_complete", {
                                'table': table_name,
                                'generated': len(data),
                                'inserted': len(data) if not self.dry_run_var.get() else 0,
                                'spec_driven': False,
                                'batch': batch_name,
                                'progress_percentage': final_progress,
                                'completed_rows': completed_rows,
                                'total_rows': total_rows_to_generate,
                                'fast_mode_used': fast_generation_enabled,
                                'duration': table_duration,
                                'rows_per_second': rows_per_second,
                                'parallel_used': use_parallel
                            }))
                
                # Verify integrity if requested
                if self.verify_var.get() and not self.dry_run_var.get():
                    self.result_queue.put(("progress", "Verifying data integrity..."))
                    integrity_report = inserter.verify_data_integrity()
                    self.result_queue.put(("integrity_check", integrity_report))
                
                # Calculate generation duration and final summary
                end_time = time.time()
                generation_duration = end_time - start_time if 'start_time' in locals() else 0
                overall_rows_per_second = total_generated / generation_duration if generation_duration > 0 else 0
                
                logger.info(f"🎉 Generation Complete!")
                logger.info(f"📊 Total rows generated: {total_generated:,}")
                logger.info(f"💾 Total rows inserted: {total_inserted:,}")
                logger.info(f"⏱️ Total duration: {generation_duration:.2f} seconds")
                logger.info(f"⚡ Overall speed: {overall_rows_per_second:,.0f} rows/second")
                logger.info(f"🧵 Thread {thread_id}: Completed successfully")
                
                self.result_queue.put(("generation_complete", {
                    'total_generated': total_generated,
                    'total_inserted': total_inserted,
                    'duration': generation_duration,
                    'fast_mode_used': fast_generation_enabled,
                    'overall_rows_per_second': overall_rows_per_second,
                    'spec_driven': self.spec_driven_var.get(),
                    'dependency_aware': self.dependency_aware_var.get(),
                    'tables_analyzed': len(table_specs) if table_specs else 0,
                    'dependency_batches': len(insertion_plan.get_insertion_batches()) if insertion_plan else 0,
                    'tables_processed': len(table_configs)
                }))
                
            except Exception as e:
                logger.error(f"💥 Critical error in generation thread: {str(e)}")
                logger.error(f"🔍 Error type: {type(e).__name__}")
                import traceback
                logger.error(f"📋 Traceback: {traceback.format_exc()}")
                
                # Reset UI state
                self.result_queue.put(("generation_error", {
                    'error': str(e),
                    'error_type': type(e).__name__,
                    'total_generated': total_generated if 'total_generated' in locals() else 0,
                    'total_inserted': total_inserted if 'total_inserted' in locals() else 0
                }))
        
        # Start generation
        self.stop_generation_flag.clear()  # Reset stop flag
        self.generate_button.config(state=tk.DISABLED, text="Generating...")
        self.stop_button.config(state=tk.NORMAL)  # Enable stop button
        self.progress_bar['value'] = 0  # Reset progress bar
        self.progress_percentage_label.config(text="0%")
        self.progress_bar.start()  # This will be stopped and switched to determinate mode once we have progress data
        
        thread = threading.Thread(target=generation_task)
        thread.daemon = True
        thread.start()
    
    def stop_generation(self):
        """Stop the currently running generation process."""
        logger.info("🛑 User requested generation stop")
        self.stop_generation_flag.set()
        
        # Update UI immediately
        self.stop_button.config(state=tk.DISABLED)
        self.progress_label.config(text="Stopping generation...")
        self.result_queue.put(("progress", "🛑 Stopping generation process..."))
        
        # We'll handle the actual stopping in the generation thread
    
    def build_generation_config(self) -> GenerationConfig:
        """Build generation configuration from GUI settings."""
        use_existing_tables = []
        
        # Get performance settings
        max_workers = int(self.max_workers_var.get()) if hasattr(self, 'max_workers_var') and self.max_workers_var.get().isdigit() else 4
        enable_multiprocessing = getattr(self, 'enable_multiprocessing_var', tk.BooleanVar()).get()
        max_processes = int(self.max_processes_var.get()) if hasattr(self, 'max_processes_var') and self.max_processes_var.get().isdigit() else 2
        rows_per_process = int(self.rows_per_process_var.get()) if hasattr(self, 'rows_per_process_var') and self.rows_per_process_var.get().isdigit() else 100000
        
        config = GenerationConfig(
            batch_size=int(self.batch_size_var.get()) if self.batch_size_var.get() else 1000,
            max_workers=max_workers,
            enable_multiprocessing=enable_multiprocessing,
            max_processes=max_processes,
            rows_per_process=rows_per_process,
            seed=int(self.seed_var.get()) if self.seed_var.get() else None,
            truncate_existing=self.truncate_var.get(),
            use_existing_tables=use_existing_tables
        )
        
        # Parse duplicate column configuration
        duplicate_config = {}
        if hasattr(self, 'enable_duplicates_var') and self.enable_duplicates_var.get():
            duplicate_columns_text = getattr(self, 'duplicate_columns_var', tk.StringVar()).get().strip()
            if duplicate_columns_text:
                for entry in duplicate_columns_text.split(','):
                    entry = entry.strip()
                    if '.' in entry:
                        table_name, column_name = entry.split('.', 1)
                        table_name, column_name = table_name.strip(), column_name.strip()
                        if table_name not in duplicate_config:
                            duplicate_config[table_name] = []
                        duplicate_config[table_name].append(column_name)

        # Add table configurations (only for selected tables)
        for item in self.config_tree.get_children():
            values = self.config_tree.item(item, "values")
            selected = values[0] == "☑️"
            table_name = values[1]  # Table name at index 1
            mode = values[2]  # Data Mode at index 2
            duplicate_mode = values[3] if len(values) > 3 else "Generate New"  # Duplicate Mode at index 3
            rows_to_generate = int(values[4]) if len(values) > 4 and values[4].isdigit() else 0  # Rows at index 4
            
            # Only include selected tables
            if selected:
                # Create table config
                if mode == "Use Existing":
                    use_existing_tables.append(table_name)
                    table_config = TableGenerationConfig(
                        rows_to_generate=0,
                        use_existing_data=True
                    )
                else:
                    table_config = TableGenerationConfig(
                        rows_to_generate=rows_to_generate,
                        use_existing_data=False
                    )
                
                # Add duplicate mode configurations from GUI
                if duplicate_mode and duplicate_mode != "Generate New":
                    # Use user's column selection if available, otherwise detect all allowed columns
                    user_selected_columns = []
                    if (hasattr(self, '_duplicate_column_selections') and 
                        table_name in self._duplicate_column_selections):
                        user_selected_columns = self._duplicate_column_selections[table_name]['columns']
                    
                    # If no user selection, fall back to all allowed columns (backward compatibility)
                    if not user_selected_columns:
                        user_selected_columns = self._get_duplicate_allowed_columns(table_name)
                    
                    # Store duplicate info for logging during generation (safer for threading)
                    if not hasattr(table_config, '_duplicate_info'):
                        table_config._duplicate_info = {
                            'mode': duplicate_mode,
                            'columns': user_selected_columns
                        }
                    
                    for column_name in user_selected_columns:
                        if duplicate_mode == "Allow Duplicates":
                            table_config.column_configs[column_name] = ColumnGenerationConfig(
                                duplicate_mode="allow_duplicates"
                            )
                        elif duplicate_mode == "Smart Duplicates":
                            table_config.column_configs[column_name] = ColumnGenerationConfig(
                                duplicate_mode="smart_duplicates",
                                duplicate_probability=0.7,
                                max_duplicate_values=5
                            )
                
                # Add duplicate column configurations from old method (backward compatibility)
                if table_name in duplicate_config:
                    for column_name in duplicate_config[table_name]:
                        table_config.column_configs[column_name] = ColumnGenerationConfig(
                            duplicate_mode="allow_duplicates"
                        )
                
                config.table_configs[table_name] = table_config
        
        # Apply global duplicate settings if enabled for maximum speed
        if hasattr(self, 'enable_duplicates_var') and self.enable_duplicates_var.get():
            # Enable global duplicate mode for faster generation
            config.duplicate_allowed = True
            config.global_duplicate_mode = "allow_duplicates"
            config.global_duplicate_probability = 1.0  # 100% duplicates for maximum speed
            
            logger.info(f"🔄 Global duplicate mode enabled for maximum generation speed")
            
            # Also enable fast generation mode automatically when duplicates are enabled
            if hasattr(self, 'fast_generation_var'):
                self.fast_generation_var.set(True)
                logger.info(f"🚀 Fast generation mode auto-enabled with duplicates")
        
        return config
    
    def get_db_config(self, for_server_connection=False) -> DatabaseConfig:
        """Get database configuration from GUI inputs."""
        return DatabaseConfig(
            host=self.host_var.get(),
            port=int(self.port_var.get()),
            database="" if for_server_connection else self.database_var.get(),
            username=self.username_var.get(),
            password=self.password_var.get(),
            driver=self.driver_var.get()
        )
    
    def fetch_available_databases(self, config: DatabaseConfig) -> list:
        """Fetch list of available databases from the server."""
        try:
            # Create a temporary connection to fetch database list
            temp_conn = DatabaseConnection(config)
            temp_conn.connect()  # Explicitly connect to the server
            
            databases = []
            with temp_conn.get_session() as session:
                if config.driver == "mysql":
                    result = session.execute(text("SHOW DATABASES"))
                    databases = [row[0] for row in result if row[0] not in 
                               ['information_schema', 'performance_schema', 'mysql', 'sys']]
                elif config.driver == "postgresql":
                    result = session.execute(text("""
                        SELECT datname FROM pg_database 
                        WHERE datistemplate = false AND datname != 'postgres'
                        ORDER BY datname
                    """))
                    databases = [row[0] for row in result]
                elif config.driver == "sqlite":
                    # For SQLite, since it's file-based, we can't list "databases" 
                    # Instead, we'll return the current database name if valid
                    if config.database:
                        databases = [config.database]
                    else:
                        databases = []
                else:
                    raise ValueError(f"Unsupported database driver: {config.driver}")
            
            temp_conn.close()
            return sorted(databases)
            
        except Exception as e:
            raise ConnectionError(f"Failed to fetch databases: {str(e)}")
    
    def refresh_databases(self):
        """Refresh the database list."""
        try:
            # Get current connection config
            config = self.get_db_config(for_server_connection=True)
            
            # Fetch databases
            databases = self.fetch_available_databases(config)
            
            if databases:
                # Remember current selection
                current_selection = self.database_var.get()
                
                # Update dropdown
                self.database_combo['values'] = databases
                
                # Restore selection if still valid
                if current_selection and current_selection in databases:
                    self.database_var.set(current_selection)
                elif databases:
                    self.database_var.set(databases[0])
                
                self.connection_status.config(text=f"✅ Refreshed - {len(databases)} databases found", foreground="green")
                messagebox.showinfo("Success", f"Database list refreshed!\nFound {len(databases)} databases.")
            else:
                raise ConnectionError("No databases found")
                
        except Exception as e:
            messagebox.showerror("Refresh Error", f"Failed to refresh database list:\n{str(e)}")
    
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
                    # Update progress bar and percentage if available
                    if 'progress_percentage' in data:
                        self.progress_bar['value'] = data['progress_percentage']
                        self.progress_percentage_label.config(text=f"{data['progress_percentage']:.1f}%")
                        
                        # Update progress label with detailed info
                        if 'completed_rows' in data and 'total_rows' in data:
                            progress_text = f"Progress: {data['completed_rows']:,} / {data['total_rows']:,} rows ({data['progress_percentage']:.1f}%)"
                            self.progress_label.config(text=progress_text)
                    
                    # Format message with fast mode indicator
                    fast_indicator = " 🚀" if data.get('fast_mode_used', False) else ""
                    message = f"✅ {data['table']}: Generated {data['generated']:,}, Inserted {data['inserted']:,}{fast_indicator}\n"
                    self.append_to_results(message)
                
                elif result_type == "generation_complete":
                    self.generate_button.config(state=tk.NORMAL, text="🎲 Generate Data")
                    self.stop_button.config(state=tk.DISABLED)  # Disable stop button
                    self.progress_bar.stop()
                    self.progress_bar['value'] = 100  # Complete
                    self.progress_percentage_label.config(text="100%")
                    self.progress_label.config(text="Generation completed!")
                    
                    # Enhanced summary with performance info
                    duration = data.get('duration', 0)
                    fast_mode_used = data.get('fast_mode_used', False)
                    
                    summary = f"\n🎉 Generation Summary:\n"
                    summary += f"  Total Generated: {data['total_generated']:,} rows\n"
                    summary += f"  Total Inserted: {data['total_inserted']:,} rows\n"
                    if duration > 0:
                        rows_per_second = data['total_generated'] / duration
                        summary += f"  Duration: {duration:.2f} seconds\n"
                        summary += f"  Speed: {rows_per_second:,.0f} rows/second\n"
                    if fast_mode_used:
                        summary += f"  🚀 Fast Generation Mode: Enabled\n"
                    summary += f"  Completion: 100% ✅\n"
                    self.append_to_results(summary)
                
                elif result_type == "generation_stopped":
                    self.generate_button.config(state=tk.NORMAL, text="🎲 Generate Data")
                    self.stop_button.config(state=tk.DISABLED)  # Disable stop button
                    self.progress_bar.stop()
                    self.progress_label.config(text="Generation stopped by user")
                    
                    # Show stopping summary
                    summary = f"\n🛑 Generation Stopped:\n"
                    summary += f"  Generated: {data['total_generated']:,} rows\n"
                    summary += f"  Inserted: {data['total_inserted']:,} rows\n"
                    summary += f"  Status: {data['message']}\n"
                    if 'last_table' in data:
                        summary += f"  Last Table: {data['last_table']}\n"
                    summary += f"  You can restart generation at any time.\n"
                    self.append_to_results(summary)
                
                elif result_type == "generation_error":
                    self.generate_button.config(state=tk.NORMAL, text="🎲 Generate Data")
                    self.stop_button.config(state=tk.DISABLED)  # Disable stop button
                    self.progress_bar.stop()
                    self.progress_label.config(text="Generation failed - check logs")
                    
                    # Show error summary
                    summary = f"\n💥 Generation Error:\n"
                    summary += f"  Error: {data['error']}\n"
                    summary += f"  Type: {data['error_type']}\n"
                    summary += f"  Generated: {data['total_generated']:,} rows\n"
                    summary += f"  Inserted: {data['total_inserted']:,} rows\n"
                    summary += f"  Check the logs tab for detailed error information.\n"
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
    """Launch the JaySoft-DBMocker GUI application."""
    root = tk.Tk()
    app = DBMockerGUI(root)
    root.mainloop()


if __name__ == "__main__":
    launch_gui()
