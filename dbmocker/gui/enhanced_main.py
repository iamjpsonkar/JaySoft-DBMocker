"""
Enhanced GUI for DBMocker with Ultra-Performance Features
Includes all new features: multi-threading, ultra-fast processing, duplicate strategies,
performance monitoring, and advanced configuration options.
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog, scrolledtext
import threading
import queue
import logging
import platform
import sys
import time
import json
import yaml
import psutil
from typing import Optional, Dict, Any, List
from pathlib import Path
from dataclasses import asdict

# Import enhanced DBMocker modules
from dbmocker.core.database import DatabaseConnection, DatabaseConfig
from dbmocker.core.analyzer import SchemaAnalyzer
from dbmocker.core.enhanced_models import (
    EnhancedGenerationConfig, PerformanceMode, DuplicateStrategy,
    InsertionStrategy, CacheStrategy, PerformanceSettings,
    DuplicateConfiguration, OptimizationHints,
    create_high_performance_config, create_bulk_generation_request
)
from dbmocker.core.high_performance_generator import HighPerformanceGenerator
from dbmocker.core.ultra_fast_processor import UltraFastProcessor, create_ultra_fast_processor


logger = logging.getLogger(__name__)


class ModernToolTip:
    """Modern styled tooltip with better appearance."""
    
    _active_tooltip = None
    
    def __init__(self, widget, text, delay=1000):
        self.widget = widget
        self.text = text
        self.delay = delay
        self.tooltip_window = None
        self.hover_job = None
        
        self.widget.bind("<Enter>", self.on_enter)
        self.widget.bind("<Leave>", self.on_leave)
    
    def on_enter(self, event=None):
        if self.hover_job:
            self.widget.after_cancel(self.hover_job)
        self.hover_job = self.widget.after(self.delay, self.show_tooltip)
    
    def on_leave(self, event=None):
        if self.hover_job:
            self.widget.after_cancel(self.hover_job)
            self.hover_job = None
        self.hide_tooltip()
    
    def show_tooltip(self, event=None):
        if self.tooltip_window or not self.text:
            return
        
        if ModernToolTip._active_tooltip and ModernToolTip._active_tooltip != self:
            ModernToolTip._active_tooltip.hide_tooltip()
        
        ModernToolTip._active_tooltip = self
        
        x = self.widget.winfo_rootx() + 25
        y = self.widget.winfo_rooty() + 25
        
        self.tooltip_window = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        
        # Modern styling
        tw.configure(bg="#2b2b2b")
        
        # Create frame with rounded appearance
        frame = tk.Frame(tw, bg="#2b2b2b", relief=tk.SOLID, borderwidth=1)
        frame.pack(padx=1, pady=1)
        
        label = tk.Label(frame, text=self.text, justify=tk.LEFT,
                        background="#2b2b2b", foreground="white",
                        font=("Segoe UI", 9), padx=8, pady=4)
        label.pack()
    
    def hide_tooltip(self):
        if self.tooltip_window:
            self.tooltip_window.destroy()
            self.tooltip_window = None
        
        if ModernToolTip._active_tooltip == self:
            ModernToolTip._active_tooltip = None


class SystemMonitor:
    """Real-time system monitoring widget."""
    
    def __init__(self, parent):
        self.parent = parent
        self.frame = ttk.LabelFrame(parent, text="🖥️ System Monitor", padding=10)
        
        # System info display
        self.cpu_var = tk.StringVar(value="CPU: 0%")
        self.memory_var = tk.StringVar(value="Memory: 0%")
        self.disk_var = tk.StringVar(value="Disk: 0%")
        
        ttk.Label(self.frame, textvariable=self.cpu_var).pack(anchor=tk.W)
        ttk.Label(self.frame, textvariable=self.memory_var).pack(anchor=tk.W)
        ttk.Label(self.frame, textvariable=self.disk_var).pack(anchor=tk.W)
        
        # Performance metrics
        self.perf_frame = ttk.Frame(self.frame)
        self.perf_frame.pack(fill=tk.X, pady=(10, 0))
        
        self.rows_per_sec_var = tk.StringVar(value="Rate: 0 rows/s")
        self.total_rows_var = tk.StringVar(value="Total: 0 rows")
        
        ttk.Label(self.perf_frame, textvariable=self.rows_per_sec_var, 
                 font=("Segoe UI", 10, "bold")).pack(anchor=tk.W)
        ttk.Label(self.perf_frame, textvariable=self.total_rows_var).pack(anchor=tk.W)
        
        # Start monitoring
        self.update_system_info()
    
    def update_system_info(self):
        """Update system information."""
        try:
            cpu_percent = psutil.cpu_percent()
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            self.cpu_var.set(f"CPU: {cpu_percent:.1f}%")
            self.memory_var.set(f"Memory: {memory.percent:.1f}%")
            self.disk_var.set(f"Disk: {disk.percent:.1f}%")
            
        except Exception as e:
            logger.debug(f"System monitoring error: {e}")
        
        # Schedule next update
        self.parent.after(2000, self.update_system_info)
    
    def update_performance(self, rows_per_sec: float, total_rows: int):
        """Update performance metrics."""
        self.rows_per_sec_var.set(f"Rate: {rows_per_sec:,.0f} rows/s")
        self.total_rows_var.set(f"Total: {total_rows:,} rows")


class PerformanceConfigPanel:
    """Advanced performance configuration panel."""
    
    def __init__(self, parent):
        self.parent = parent
        self.frame = ttk.LabelFrame(parent, text="⚡ Performance Configuration", padding=10)
        
        # Performance mode selection
        mode_frame = ttk.Frame(self.frame)
        mode_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(mode_frame, text="Performance Mode:", font=("Segoe UI", 10, "bold")).pack(anchor=tk.W)
        
        self.performance_mode = tk.StringVar(value="balanced")
        mode_options = [
            ("Standard", "standard"),
            ("High Speed", "high_speed"),
            ("Memory Efficient", "memory_efficient"),
            ("Balanced", "balanced"),
            ("Ultra High", "ultra_high")
        ]
        
        for i, (text, value) in enumerate(mode_options):
            rb = ttk.Radiobutton(mode_frame, text=text, variable=self.performance_mode, value=value)
            rb.pack(anchor=tk.W, padx=(20, 0))
            
            # Add tooltips
            tooltips = {
                "standard": "Basic generation with standard performance",
                "high_speed": "Optimized for maximum generation speed",
                "memory_efficient": "Optimized for low memory usage",
                "balanced": "Balanced performance and memory usage",
                "ultra_high": "Maximum performance for millions of records"
            }
            ModernToolTip(rb, tooltips[value])
        
        # Advanced settings
        advanced_frame = ttk.LabelFrame(self.frame, text="Advanced Settings", padding=5)
        advanced_frame.pack(fill=tk.X, pady=(10, 0))
        
        # Workers and batch size
        workers_frame = ttk.Frame(advanced_frame)
        workers_frame.pack(fill=tk.X, pady=2)
        
        ttk.Label(workers_frame, text="Max Workers:").pack(side=tk.LEFT)
        self.max_workers = tk.IntVar(value=psutil.cpu_count())
        workers_spinbox = ttk.Spinbox(workers_frame, from_=1, to=32, width=10, 
                                     textvariable=self.max_workers)
        workers_spinbox.pack(side=tk.LEFT, padx=(10, 0))
        ModernToolTip(workers_spinbox, "Number of parallel worker threads")
        
        # Batch size
        batch_frame = ttk.Frame(advanced_frame)
        batch_frame.pack(fill=tk.X, pady=2)
        
        ttk.Label(batch_frame, text="Batch Size:").pack(side=tk.LEFT)
        self.batch_size = tk.IntVar(value=10000)
        batch_spinbox = ttk.Spinbox(batch_frame, from_=1000, to=100000, width=10,
                                   textvariable=self.batch_size)
        batch_spinbox.pack(side=tk.LEFT, padx=(10, 0))
        ModernToolTip(batch_spinbox, "Number of rows processed in each batch")
        
        # Enable multiprocessing
        self.enable_multiprocessing = tk.BooleanVar(value=False)
        mp_check = ttk.Checkbutton(advanced_frame, text="Enable Multiprocessing",
                                  variable=self.enable_multiprocessing)
        mp_check.pack(anchor=tk.W, pady=2)
        ModernToolTip(mp_check, "Use multiple processes for very large datasets")
        
        # Cache settings
        cache_frame = ttk.LabelFrame(advanced_frame, text="Cache Settings", padding=5)
        cache_frame.pack(fill=tk.X, pady=(5, 0))
        
        cache_size_frame = ttk.Frame(cache_frame)
        cache_size_frame.pack(fill=tk.X, pady=2)
        
        ttk.Label(cache_size_frame, text="Cache Size (MB):").pack(side=tk.LEFT)
        self.cache_size = tk.IntVar(value=500)
        cache_spinbox = ttk.Spinbox(cache_size_frame, from_=100, to=2000, width=10,
                                   textvariable=self.cache_size)
        cache_spinbox.pack(side=tk.LEFT, padx=(10, 0))
        ModernToolTip(cache_spinbox, "Memory allocated for intelligent caching")
    
    def get_performance_config(self) -> PerformanceSettings:
        """Get performance configuration."""
        return PerformanceSettings(
            performance_mode=PerformanceMode(self.performance_mode.get()),
            max_workers=self.max_workers.get(),
            enable_multiprocessing=self.enable_multiprocessing.get(),
            cache_size_mb=self.cache_size.get(),
            batch_size=self.batch_size.get()
        )


class DuplicateConfigPanel:
    """Advanced duplicate handling configuration panel."""
    
    def __init__(self, parent):
        self.parent = parent
        self.frame = ttk.LabelFrame(parent, text="🔄 Duplicate Configuration", padding=10)
        
        # Enable duplicates
        self.enable_duplicates = tk.BooleanVar(value=False)
        enable_check = ttk.Checkbutton(self.frame, text="Enable Duplicate Generation",
                                      variable=self.enable_duplicates,
                                      command=self.toggle_duplicate_options)
        enable_check.pack(anchor=tk.W, pady=(0, 10))
        ModernToolTip(enable_check, "Allow duplicate values to create realistic data patterns")
        
        # Duplicate strategy frame
        self.strategy_frame = ttk.LabelFrame(self.frame, text="Duplicate Strategy", padding=5)
        self.strategy_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.duplicate_strategy = tk.StringVar(value="smart_duplicates")
        strategies = [
            ("Generate New", "generate_new", "Always generate unique values"),
            ("Allow Simple", "allow_simple", "Allow simple duplicate values"),
            ("Smart Duplicates", "smart_duplicates", "Intelligent distribution of duplicates"),
            ("Cached Pool", "cached_pool", "Use cached value pools for maximum performance"),
            ("Weighted Random", "weighted_random", "Weighted random selection from value pools"),
            ("Fast Data Reuse", "fast_data_reuse", "Ultra-fast reuse of existing data (fastest option)")
        ]
        
        for text, value, tooltip in strategies:
            rb = ttk.Radiobutton(self.strategy_frame, text=text, 
                                variable=self.duplicate_strategy, value=value,
                                command=self.on_strategy_change)
            rb.pack(anchor=tk.W, padx=(10, 0))
            ModernToolTip(rb, tooltip)
        
        # Advanced duplicate settings
        self.advanced_frame = ttk.LabelFrame(self.frame, text="Advanced Settings", padding=5)
        self.advanced_frame.pack(fill=tk.X)
        
        # Duplicate probability
        prob_frame = ttk.Frame(self.advanced_frame)
        prob_frame.pack(fill=tk.X, pady=2)
        
        ttk.Label(prob_frame, text="Duplicate Probability:").pack(side=tk.LEFT)
        self.duplicate_probability = tk.DoubleVar(value=0.3)
        prob_scale = ttk.Scale(prob_frame, from_=0.0, to=1.0, variable=self.duplicate_probability,
                              orient=tk.HORIZONTAL, length=150)
        prob_scale.pack(side=tk.LEFT, padx=(10, 5))
        
        self.prob_label = ttk.Label(prob_frame, text="30%")
        self.prob_label.pack(side=tk.LEFT)
        
        prob_scale.configure(command=self.update_probability_label)
        ModernToolTip(prob_scale, "Probability of generating duplicate values")
        
        # Pool sizes
        pool_frame = ttk.Frame(self.advanced_frame)
        pool_frame.pack(fill=tk.X, pady=2)
        
        ttk.Label(pool_frame, text="Small Pool Size:").pack(side=tk.LEFT)
        self.pool_size_small = tk.IntVar(value=5)
        small_spinbox = ttk.Spinbox(pool_frame, from_=2, to=50, width=8,
                                   textvariable=self.pool_size_small)
        small_spinbox.pack(side=tk.LEFT, padx=(10, 20))
        ModernToolTip(small_spinbox, "Pool size for high-frequency values")
        
        ttk.Label(pool_frame, text="Large Pool:").pack(side=tk.LEFT)
        self.pool_size_large = tk.IntVar(value=100)
        large_spinbox = ttk.Spinbox(pool_frame, from_=10, to=1000, width=8,
                                   textvariable=self.pool_size_large)
        large_spinbox.pack(side=tk.LEFT, padx=(10, 0))
        ModernToolTip(large_spinbox, "Pool size for diverse values")
        
        # Fast data reuse settings
        reuse_frame = ttk.LabelFrame(self.advanced_frame, text="Fast Data Reuse Settings", padding=5)
        reuse_frame.pack(fill=tk.X, pady=(5, 0))
        
        # Sample size
        sample_frame = ttk.Frame(reuse_frame)
        sample_frame.pack(fill=tk.X, pady=2)
        
        ttk.Label(sample_frame, text="Sample Size:").pack(side=tk.LEFT)
        self.sample_size = tk.IntVar(value=10000)
        sample_spinbox = ttk.Spinbox(sample_frame, from_=1000, to=100000, width=10,
                                    textvariable=self.sample_size)
        sample_spinbox.pack(side=tk.LEFT, padx=(10, 20))
        ModernToolTip(sample_spinbox, "Number of existing rows to sample for reuse")
        
        # Data reuse probability
        reuse_prob_frame = ttk.Frame(reuse_frame)
        reuse_prob_frame.pack(fill=tk.X, pady=2)
        
        ttk.Label(reuse_prob_frame, text="Reuse Probability:").pack(side=tk.LEFT)
        self.data_reuse_probability = tk.DoubleVar(value=0.95)
        reuse_prob_scale = ttk.Scale(reuse_prob_frame, from_=0.0, to=1.0, 
                                    variable=self.data_reuse_probability,
                                    orient=tk.HORIZONTAL, length=120)
        reuse_prob_scale.pack(side=tk.LEFT, padx=(10, 5))
        
        self.reuse_prob_label = ttk.Label(reuse_prob_frame, text="95%")
        self.reuse_prob_label.pack(side=tk.LEFT)
        
        reuse_prob_scale.configure(command=self.update_reuse_probability_label)
        ModernToolTip(reuse_prob_scale, "Probability of reusing existing data vs generating new")
        
        # Progress update interval
        progress_frame = ttk.Frame(reuse_frame)
        progress_frame.pack(fill=tk.X, pady=2)
        
        ttk.Label(progress_frame, text="Progress Interval:").pack(side=tk.LEFT)
        self.progress_interval = tk.IntVar(value=1000)
        progress_spinbox = ttk.Spinbox(progress_frame, from_=100, to=10000, width=8,
                                      textvariable=self.progress_interval)
        progress_spinbox.pack(side=tk.LEFT, padx=(10, 0))
        ModernToolTip(progress_spinbox, "Progress update interval (every N rows)")
        
        # Fast insertion mode
        self.fast_insertion_mode = tk.BooleanVar(value=True)
        fast_check = ttk.Checkbutton(reuse_frame, text="Enable Fast Insertion Mode",
                                    variable=self.fast_insertion_mode)
        fast_check.pack(anchor=tk.W, pady=2)
        ModernToolTip(fast_check, "Use fastest possible insertion optimizations")
        
        # Initially disable duplicate options
        self.toggle_duplicate_options()
    
    def update_probability_label(self, value):
        """Update probability label."""
        prob = float(value)
        self.prob_label.config(text=f"{prob*100:.0f}%")
    
    def update_reuse_probability_label(self, value):
        """Update reuse probability label."""
        prob = float(value)
        self.reuse_prob_label.config(text=f"{prob*100:.0f}%")
    
    def on_strategy_change(self):
        """Handle strategy change to suggest optimal settings."""
        # This method will be called from the main GUI
        pass
    
    def toggle_duplicate_options(self):
        """Toggle duplicate options based on enable checkbox."""
        state = tk.NORMAL if self.enable_duplicates.get() else tk.DISABLED
        
        for child in self.strategy_frame.winfo_children():
            if isinstance(child, ttk.Radiobutton):
                child.configure(state=state)
        
        for child in self.advanced_frame.winfo_children():
            if isinstance(child, ttk.Frame):
                for subchild in child.winfo_children():
                    if isinstance(subchild, (ttk.Scale, ttk.Spinbox)):
                        subchild.configure(state=state)
    
    def get_duplicate_config(self) -> DuplicateConfiguration:
        """Get duplicate configuration."""
        return DuplicateConfiguration(
            global_duplicate_enabled=self.enable_duplicates.get(),
            global_duplicate_strategy=DuplicateStrategy(self.duplicate_strategy.get()) if self.enable_duplicates.get() else DuplicateStrategy.GENERATE_NEW,
            global_duplicate_probability=self.duplicate_probability.get(),
            pool_size_small=self.pool_size_small.get(),
            pool_size_large=self.pool_size_large.get(),
            # Fast data reuse settings
            enable_fast_data_reuse=(self.duplicate_strategy.get() == "fast_data_reuse"),
            data_reuse_sample_size=self.sample_size.get(),
            data_reuse_probability=self.data_reuse_probability.get(),
            respect_constraints=True,  # Always respect constraints
            fast_insertion_mode=self.fast_insertion_mode.get(),
            progress_update_interval=self.progress_interval.get()
        )


class TableConfigPanel:
    """Enhanced table configuration panel with row count settings."""
    
    def __init__(self, parent):
        self.parent = parent
        self.frame = ttk.LabelFrame(parent, text="📊 Table Configuration", padding=10)
        
        # Table selection and configuration
        self.table_configs = {}
        self.create_table_config_ui()
    
    def create_table_config_ui(self):
        """Create table configuration UI with improved scrolling."""
        # Instructions
        instruction_label = ttk.Label(self.frame, 
                                     text="Configure row counts for each table:",
                                     font=("Segoe UI", 10, "bold"))
        instruction_label.pack(anchor=tk.W, pady=(0, 10))
        
        # Container for scrollable area
        scroll_container = ttk.Frame(self.frame)
        scroll_container.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        # Scrollable frame with both vertical and horizontal scrollbars
        self.canvas = tk.Canvas(scroll_container, height=300, highlightthickness=0)
        
        # Vertical scrollbar
        v_scrollbar = ttk.Scrollbar(scroll_container, orient="vertical", command=self.canvas.yview)
        v_scrollbar.pack(side="right", fill="y")
        
        # Horizontal scrollbar
        h_scrollbar = ttk.Scrollbar(scroll_container, orient="horizontal", command=self.canvas.xview)
        h_scrollbar.pack(side="bottom", fill="x")
        
        # Configure canvas scrolling
        self.canvas.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        self.canvas.pack(side="left", fill="both", expand=True)
        
        # Scrollable frame inside canvas
        self.scrollable_frame = ttk.Frame(self.canvas)
        self.canvas_window = self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        
        # Bind events for proper scrolling
        self.scrollable_frame.bind("<Configure>", self.on_frame_configure)
        self.canvas.bind("<Configure>", self.on_canvas_configure)
        
        # Enable mouse wheel scrolling
        self.canvas.bind("<MouseWheel>", self.on_mousewheel)
        self.canvas.bind("<Button-4>", self.on_mousewheel)
        self.canvas.bind("<Button-5>", self.on_mousewheel)
        
        # Quick presets
        preset_frame = ttk.LabelFrame(self.frame, text="Quick Presets", padding=5)
        preset_frame.pack(fill=tk.X, pady=(10, 0))
        
        preset_buttons = [
            ("Small (1K)", 1000),
            ("Medium (10K)", 10000),
            ("Large (100K)", 100000),
            ("Very Large (1M)", 1000000)
        ]
        
        button_frame = ttk.Frame(preset_frame)
        button_frame.pack(fill=tk.X)
        
        for text, count in preset_buttons:
            btn = ttk.Button(button_frame, text=text, 
                           command=lambda c=count: self.apply_preset(c))
            btn.pack(side=tk.LEFT, padx=(0, 5))
            ModernToolTip(btn, f"Set all tables to {count:,} rows")
    
    def on_frame_configure(self, event):
        """Handle frame configure event for scrolling."""
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))
    
    def on_canvas_configure(self, event):
        """Handle canvas configure event for proper sizing."""
        # Update the scrollable frame width to match canvas width if needed
        canvas_width = event.width
        self.canvas.itemconfig(self.canvas_window, width=canvas_width)
    
    def on_mousewheel(self, event):
        """Handle mouse wheel scrolling."""
        if event.delta:
            # Windows and MacOS
            self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        elif event.num == 4:
            # Linux scroll up
            self.canvas.yview_scroll(-1, "units")
        elif event.num == 5:
            # Linux scroll down
            self.canvas.yview_scroll(1, "units")
    
    def update_tables(self, schema):
        """Update table list from schema."""
        # Clear existing configs
        for widget in self.scrollable_frame.winfo_children():
            widget.destroy()
        
        self.table_configs.clear()
        
        if not schema:
            return
        
        # Create a grid layout for better handling of many tables
        tables_per_row = 2  # Show 2 tables per row for better space utilization
        current_row_frame = None
        
        for i, table in enumerate(schema.tables):
            # Create new row frame every 2 tables
            if i % tables_per_row == 0:
                current_row_frame = ttk.Frame(self.scrollable_frame)
                current_row_frame.pack(fill=tk.X, pady=2)
            
            # Table configuration frame
            table_frame = ttk.LabelFrame(current_row_frame, text=f"📊 {table.name}", padding=5)
            table_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5) if i % tables_per_row == 0 else (5, 0))
            
            # Table info
            info_label = ttk.Label(table_frame, 
                                  text=f"{len(table.columns)} columns • {len(table.foreign_keys)} FKs",
                                  font=("Segoe UI", 8), foreground="gray")
            info_label.pack(anchor=tk.W, pady=(0, 5))
            
            # Row count input with better layout
            count_frame = ttk.Frame(table_frame)
            count_frame.pack(fill=tk.X)
            
            ttk.Label(count_frame, text="Rows:", font=("Segoe UI", 9, "bold")).pack(side=tk.LEFT)
            
            row_count_var = tk.IntVar(value=10000)  # Default 10K rows
            spinbox = ttk.Spinbox(count_frame, from_=0, to=100000000, width=15,
                                 textvariable=row_count_var, format="%d")
            spinbox.pack(side=tk.RIGHT, fill=tk.X, expand=True, padx=(10, 0))
            
            self.table_configs[table.name] = row_count_var
            
            ModernToolTip(spinbox, f"Number of rows to generate for {table.name}\nColumns: {', '.join([col.name for col in table.columns[:3]])}{'...' if len(table.columns) > 3 else ''}")
        
        # Update scroll region after adding all tables
        self.scrollable_frame.update_idletasks()
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))
    
    def apply_preset(self, count):
        """Apply preset row count to all tables."""
        for var in self.table_configs.values():
            var.set(count)
    
    def get_table_configs(self) -> Dict[str, int]:
        """Get table configurations."""
        return {name: var.get() for name, var in self.table_configs.items()}


class ProgressMonitor:
    """Advanced progress monitoring with detailed metrics."""
    
    def __init__(self, parent):
        self.parent = parent
        self.frame = ttk.LabelFrame(parent, text="📈 Progress Monitor", padding=10)
        
        # Overall progress
        self.overall_progress = ttk.Progressbar(self.frame, mode='determinate', length=400)
        self.overall_progress.pack(fill=tk.X, pady=(0, 10))
        
        # Status labels
        self.status_var = tk.StringVar(value="Ready")
        self.current_table_var = tk.StringVar(value="")
        self.rows_generated_var = tk.StringVar(value="Rows: 0")
        self.rate_var = tk.StringVar(value="Rate: 0 rows/s")
        self.eta_var = tk.StringVar(value="ETA: --")
        
        status_label = ttk.Label(self.frame, textvariable=self.status_var,
                                font=("Segoe UI", 11, "bold"))
        status_label.pack(anchor=tk.W)
        
        ttk.Label(self.frame, textvariable=self.current_table_var).pack(anchor=tk.W)
        
        # Metrics frame
        metrics_frame = ttk.Frame(self.frame)
        metrics_frame.pack(fill=tk.X, pady=(10, 0))
        
        left_frame = ttk.Frame(metrics_frame)
        left_frame.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        right_frame = ttk.Frame(metrics_frame)
        right_frame.pack(side=tk.RIGHT)
        
        ttk.Label(left_frame, textvariable=self.rows_generated_var).pack(anchor=tk.W)
        ttk.Label(right_frame, textvariable=self.rate_var).pack(anchor=tk.E)
        ttk.Label(right_frame, textvariable=self.eta_var).pack(anchor=tk.E)
        
        # Control buttons
        control_frame = ttk.Frame(self.frame)
        control_frame.pack(fill=tk.X, pady=(10, 0))
        
        self.stop_button = ttk.Button(control_frame, text="⏹️ Stop", state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT)
        
        self.pause_button = ttk.Button(control_frame, text="⏸️ Pause", state=tk.DISABLED)
        self.pause_button.pack(side=tk.LEFT, padx=(5, 0))
        
        # Initialize
        self.reset()
    
    def reset(self):
        """Reset progress monitor."""
        self.overall_progress['value'] = 0
        self.status_var.set("Ready")
        self.current_table_var.set("")
        self.rows_generated_var.set("Rows: 0")
        self.rate_var.set("Rate: 0 rows/s")
        self.eta_var.set("ETA: --")
        self.stop_button.configure(state=tk.DISABLED)
        self.pause_button.configure(state=tk.DISABLED)
    
    def start_generation(self, total_rows):
        """Start generation monitoring."""
        self.total_rows = total_rows
        self.start_time = time.time()
        self.status_var.set("🚀 Generating...")
        self.stop_button.configure(state=tk.NORMAL)
        self.pause_button.configure(state=tk.NORMAL)
    
    def update_progress(self, current_rows, current_table="", rate=0):
        """Update progress with enhanced tracking every 1000 records."""
        if hasattr(self, 'total_rows') and self.total_rows > 0:
            progress = (current_rows / self.total_rows) * 100
            self.overall_progress['value'] = progress
        
        self.rows_generated_var.set(f"Rows: {current_rows:,}")
        self.rate_var.set(f"Rate: {rate:,.0f} rows/s")
        
        if current_table:
            self.current_table_var.set(f"Current: {current_table}")
        
        # Enhanced status for every 1000 records
        if current_rows % 1000 == 0:
            elapsed_time = time.time() - self.start_time if hasattr(self, 'start_time') else 0
            avg_rate = current_rows / elapsed_time if elapsed_time > 0 else 0
            
            status_msg = f"🚀 {current_rows:,} rows | {rate:,.0f} rows/s | Avg: {avg_rate:,.0f} rows/s"
            self.status_var.set(status_msg)
        
        # Calculate ETA
        if rate > 0 and hasattr(self, 'total_rows'):
            remaining = self.total_rows - current_rows
            eta_seconds = remaining / rate
            eta_minutes = eta_seconds / 60
            
            if eta_minutes > 60:
                eta_str = f"ETA: {eta_minutes/60:.1f}h"
            elif eta_minutes > 1:
                eta_str = f"ETA: {eta_minutes:.1f}m"
            else:
                eta_str = f"ETA: {eta_seconds:.0f}s"
            
            self.eta_var.set(eta_str)
    
    def complete(self, total_time, total_rows):
        """Mark generation as complete."""
        self.overall_progress['value'] = 100
        self.status_var.set("✅ Completed")
        avg_rate = total_rows / total_time if total_time > 0 else 0
        self.rate_var.set(f"Avg: {avg_rate:,.0f} rows/s")
        self.eta_var.set(f"Time: {total_time:.1f}s")
        self.stop_button.configure(state=tk.DISABLED)
        self.pause_button.configure(state=tk.DISABLED)


class EnhancedDBMockerGUI:
    """Enhanced DBMocker GUI with ultra-performance features."""
    
    def __init__(self, root: tk.Tk):
        """Initialize the enhanced GUI."""
        self.root = root
        self.root.title("DBMocker Ultra - High-Performance Database Mock Data Generator")
        
        # Configure window
        self.configure_window()
        
        # Application state
        self.db_connection: Optional[DatabaseConnection] = None
        self.schema = None
        self.current_generator = None
        
        # Threading
        self.task_queue = queue.Queue()
        self.result_queue = queue.Queue()
        self.stop_generation_flag = threading.Event()
        
        # Setup GUI
        self.setup_modern_gui()
        self.setup_logging()
        
        # Start result processing
        self.process_results()
    
    def configure_window(self):
        """Configure main window."""
        # Set icon
        try:
            icon_path = Path(__file__).parent.parent.parent / "assets" / "logos" / "jaysoft_dbmocker_icon.png"
            if icon_path.exists():
                self.root.iconphoto(True, tk.PhotoImage(file=str(icon_path)))
        except:
            pass
        
        # Window size and position
        if platform.system() == "Darwin":  # macOS
            self.root.geometry("1600x1000")
            try:
                self.root.state('zoomed')
            except:
                pass
        else:
            self.root.state('zoomed')
        
        self.root.minsize(1200, 800)
        
        # Configure style
        self.setup_modern_style()
    
    def setup_modern_style(self):
        """Setup modern styling."""
        style = ttk.Style()
        
        # Configure modern theme
        if "clam" in style.theme_names():
            style.theme_use("clam")
        
        # Custom styles
        style.configure("Header.TLabel", font=("Segoe UI", 14, "bold"))
        style.configure("Subheader.TLabel", font=("Segoe UI", 11, "bold"))
        style.configure("Modern.TButton", padding=(10, 5))
        style.configure("Success.TButton", foreground="green")
        style.configure("Danger.TButton", foreground="red")
    
    def setup_modern_gui(self):
        """Setup the modern GUI layout."""
        # Main container
        main_container = ttk.Frame(self.root)
        main_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Header
        self.create_header(main_container)
        
        # Main content area with notebook
        self.notebook = ttk.Notebook(main_container)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=(10, 0))
        
        # Create tabs
        self.create_connection_tab()
        self.create_configuration_tab()
        self.create_generation_tab()
        self.create_monitoring_tab()
        self.create_results_tab()
        
        # Status bar
        self.create_status_bar(main_container)
    
    def create_header(self, parent):
        """Create application header."""
        header_frame = ttk.Frame(parent)
        header_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Logo and title
        title_frame = ttk.Frame(header_frame)
        title_frame.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        title_label = ttk.Label(title_frame, text="🚀 DBMocker Ultra", 
                               style="Header.TLabel")
        title_label.pack(anchor=tk.W)
        
        subtitle_label = ttk.Label(title_frame, 
                                  text="High-Performance Database Mock Data Generator",
                                  font=("Segoe UI", 10))
        subtitle_label.pack(anchor=tk.W)
        
        # System info
        system_frame = ttk.Frame(header_frame)
        system_frame.pack(side=tk.RIGHT)
        
        cpu_count = psutil.cpu_count()
        memory_gb = psutil.virtual_memory().total / (1024**3)
        
        system_label = ttk.Label(system_frame, 
                                text=f"💻 {cpu_count} CPUs • {memory_gb:.1f}GB RAM",
                                font=("Segoe UI", 9))
        system_label.pack(anchor=tk.E)
    
    def create_connection_tab(self):
        """Create database connection tab."""
        self.connection_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.connection_frame, text="🔗 Database Connection")
        
        # Connection form
        form_frame = ttk.LabelFrame(self.connection_frame, text="Database Configuration", padding=20)
        form_frame.pack(fill=tk.X, padx=20, pady=20)
        
        # Database type
        db_frame = ttk.Frame(form_frame)
        db_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(db_frame, text="Database Type:", width=15).pack(side=tk.LEFT)
        self.db_driver = tk.StringVar(value="sqlite")
        driver_combo = ttk.Combobox(db_frame, textvariable=self.db_driver,
                                   values=["sqlite", "postgresql", "mysql"],
                                   state="readonly", width=20)
        driver_combo.pack(side=tk.LEFT, padx=(10, 0))
        driver_combo.bind("<<ComboboxSelected>>", self.on_driver_change)
        
        # Connection fields (server-level)
        self.connection_fields = {}
        fields = [
            ("Host:", "host", "localhost"),
            ("Port:", "port", "5432"),
            ("Username:", "username", "postgres"),
            ("Password:", "password", "")
        ]
        
        for label, key, default in fields:
            field_frame = ttk.Frame(form_frame)
            field_frame.pack(fill=tk.X, pady=5)
            
            ttk.Label(field_frame, text=label, width=15).pack(side=tk.LEFT)
            
            if key == "password":
                entry = ttk.Entry(field_frame, show="*", width=30)
            else:
                entry = ttk.Entry(field_frame, width=30)
            
            entry.pack(side=tk.LEFT, padx=(10, 0))
            entry.insert(0, default)
            
            self.connection_fields[key] = entry
        
        # Database selection (shown after server connection)
        self.db_select_frame = ttk.Frame(form_frame)
        self.db_select_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(self.db_select_frame, text="Database:", width=15).pack(side=tk.LEFT)
        self.database_var = tk.StringVar()
        self.database_combo = ttk.Combobox(self.db_select_frame, textvariable=self.database_var,
                                          state="readonly", width=30)
        self.database_combo.pack(side=tk.LEFT, padx=(10, 0))
        self.database_combo.bind("<<ComboboxSelected>>", self.on_database_selected)
        
        # Initially hide database selection
        self.db_select_frame.pack_forget()
        
        # SQLite file selection (shown for SQLite driver)
        self.sqlite_frame = ttk.Frame(form_frame)
        sqlite_file_frame = ttk.Frame(self.sqlite_frame)
        sqlite_file_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(sqlite_file_frame, text="Database File:", width=15).pack(side=tk.LEFT)
        self.sqlite_file_var = tk.StringVar(value="database.db")
        sqlite_entry = ttk.Entry(sqlite_file_frame, textvariable=self.sqlite_file_var, width=25)
        sqlite_entry.pack(side=tk.LEFT, padx=(10, 5))
        
        browse_button = ttk.Button(sqlite_file_frame, text="Browse...", 
                                  command=self.browse_sqlite_file)
        browse_button.pack(side=tk.LEFT)
        
        # Connection buttons
        button_frame = ttk.Frame(form_frame)
        button_frame.pack(fill=tk.X, pady=(20, 0))
        
        self.test_button = ttk.Button(button_frame, text="🧪 Test Connection",
                                     command=self.test_connection,
                                     style="Modern.TButton")
        self.test_button.pack(side=tk.LEFT)
        
        self.connect_server_button = ttk.Button(button_frame, text="🔗 Connect to Server",
                                               command=self.connect_to_server,
                                               style="Modern.TButton")
        self.connect_server_button.pack(side=tk.LEFT, padx=(10, 0))
        
        self.analyze_button = ttk.Button(button_frame, text="📊 Analyze Database",
                                        command=self.connect_to_database,
                                        style="Modern.TButton",
                                        state=tk.DISABLED)
        self.analyze_button.pack(side=tk.LEFT, padx=(10, 0))
        
        # Connection status
        self.connection_status = ttk.Label(form_frame, text="Not connected",
                                          font=("Segoe UI", 10))
        self.connection_status.pack(anchor=tk.W, pady=(10, 0))
        
        # Quick setup buttons
        quick_frame = ttk.LabelFrame(self.connection_frame, text="Quick Setup", padding=10)
        quick_frame.pack(fill=tk.X, padx=20, pady=(0, 20))
        
        quick_buttons = [
            ("📁 SQLite File", self.quick_sqlite),
            ("🐘 PostgreSQL Local", self.quick_postgresql),
            ("🐬 MySQL Local", self.quick_mysql)
        ]
        
        for text, command in quick_buttons:
            btn = ttk.Button(quick_frame, text=text, command=command)
            btn.pack(side=tk.LEFT, padx=(0, 10))
        
        # Initialize with SQLite settings
        self.on_driver_change()
    
    def create_configuration_tab(self):
        """Create configuration tab."""
        self.config_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.config_frame, text="⚙️ Configuration")
        
        # Create scrollable content
        canvas = tk.Canvas(self.config_frame)
        scrollbar = ttk.Scrollbar(self.config_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Layout
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Configuration panels
        left_column = ttk.Frame(scrollable_frame)
        left_column.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(20, 10), pady=20)
        
        right_column = ttk.Frame(scrollable_frame)
        right_column.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=(10, 20), pady=20)
        
        # Performance configuration
        self.performance_panel = PerformanceConfigPanel(left_column)
        self.performance_panel.frame.pack(fill=tk.X, pady=(0, 20))
        
        # Duplicate configuration
        self.duplicate_panel = DuplicateConfigPanel(left_column)
        self.duplicate_panel.frame.pack(fill=tk.X)
        
        # Connect strategy change callback
        self.duplicate_panel.on_strategy_change = self.on_duplicate_strategy_change
        
        # Table configuration
        self.table_panel = TableConfigPanel(right_column)
        self.table_panel.frame.pack(fill=tk.BOTH, expand=True, pady=(0, 20))
        
        # System monitor
        self.system_monitor = SystemMonitor(right_column)
        self.system_monitor.frame.pack(fill=tk.X)
    
    def create_generation_tab(self):
        """Create generation tab."""
        self.generation_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.generation_frame, text="🚀 Generation")
        
        # Main content
        content_frame = ttk.Frame(self.generation_frame)
        content_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Generation controls
        control_frame = ttk.LabelFrame(content_frame, text="Generation Controls", padding=20)
        control_frame.pack(fill=tk.X, pady=(0, 20))
        
        # Generation mode selection
        mode_frame = ttk.Frame(control_frame)
        mode_frame.pack(fill=tk.X, pady=(0, 15))
        
        ttk.Label(mode_frame, text="Generation Mode:", style="Subheader.TLabel").pack(anchor=tk.W)
        
        self.generation_mode = tk.StringVar(value="standard")
        modes = [
            ("Standard Generation", "standard", "Regular generation with basic optimizations"),
            ("Bulk Generation", "bulk", "Optimized bulk generation for large datasets"),
            ("Ultra-Fast Processing", "ultra_high_performance", "Maximum performance for millions of records")
        ]
        
        for text, value, tooltip in modes:
            rb = ttk.Radiobutton(mode_frame, text=text, variable=self.generation_mode, value=value)
            rb.pack(anchor=tk.W, padx=(20, 0), pady=2)
            ModernToolTip(rb, tooltip)
        
        # Generation options
        options_frame = ttk.Frame(control_frame)
        options_frame.pack(fill=tk.X, pady=(15, 0))
        
        # Streaming option
        self.use_streaming = tk.BooleanVar(value=False)
        streaming_check = ttk.Checkbutton(options_frame, text="Use Streaming Mode",
                                         variable=self.use_streaming)
        streaming_check.pack(anchor=tk.W)
        ModernToolTip(streaming_check, "Use streaming mode for memory-efficient generation")
        
        # Truncate option
        self.truncate_tables = tk.BooleanVar(value=False)
        truncate_check = ttk.Checkbutton(options_frame, text="Truncate Tables Before Generation",
                                        variable=self.truncate_tables)
        truncate_check.pack(anchor=tk.W)
        ModernToolTip(truncate_check, "Clear existing data before generating new data")
        
        # Seed option
        seed_frame = ttk.Frame(options_frame)
        seed_frame.pack(fill=tk.X, pady=(10, 0))
        
        self.use_seed = tk.BooleanVar(value=False)
        seed_check = ttk.Checkbutton(seed_frame, text="Use Random Seed:",
                                    variable=self.use_seed)
        seed_check.pack(side=tk.LEFT)
        
        self.seed_value = tk.IntVar(value=42)
        seed_entry = ttk.Entry(seed_frame, textvariable=self.seed_value, width=10)
        seed_entry.pack(side=tk.LEFT, padx=(10, 0))
        ModernToolTip(seed_entry, "Random seed for reproducible data generation")
        
        # Action buttons
        action_frame = ttk.Frame(control_frame)
        action_frame.pack(fill=tk.X, pady=(20, 0))
        
        self.generate_button = ttk.Button(action_frame, text="🚀 Start Generation",
                                         command=self.start_generation,
                                         style="Success.TButton")
        self.generate_button.pack(side=tk.LEFT)
        
        self.stop_button = ttk.Button(action_frame, text="⏹️ Stop Generation",
                                     command=self.stop_generation,
                                     style="Danger.TButton", state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=(10, 0))
        
        # Export buttons
        export_frame = ttk.Frame(action_frame)
        export_frame.pack(side=tk.RIGHT)
        
        ttk.Button(export_frame, text="💾 Save Config",
                  command=self.save_configuration).pack(side=tk.LEFT)
        ttk.Button(export_frame, text="📁 Load Config",
                  command=self.load_configuration).pack(side=tk.LEFT, padx=(5, 0))
        
        # Progress monitor
        self.progress_monitor = ProgressMonitor(content_frame)
        self.progress_monitor.frame.pack(fill=tk.X)
        
        # Connect stop button
        self.progress_monitor.stop_button.configure(command=self.stop_generation)
    
    def create_monitoring_tab(self):
        """Create monitoring tab."""
        self.monitoring_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.monitoring_frame, text="📊 Monitoring")
        
        # Real-time metrics
        metrics_frame = ttk.LabelFrame(self.monitoring_frame, text="Real-time Metrics", padding=20)
        metrics_frame.pack(fill=tk.X, padx=20, pady=20)
        
        # Create metrics display (placeholder for now)
        self.metrics_text = scrolledtext.ScrolledText(metrics_frame, height=15, width=80)
        self.metrics_text.pack(fill=tk.BOTH, expand=True)
        
        # Performance charts would go here in a full implementation
        charts_frame = ttk.LabelFrame(self.monitoring_frame, text="Performance Charts", padding=20)
        charts_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=(0, 20))
        
        chart_placeholder = ttk.Label(charts_frame, 
                                     text="📈 Performance charts will be displayed here\n"
                                          "(Requires matplotlib integration)",
                                     font=("Segoe UI", 12), foreground="gray")
        chart_placeholder.pack(expand=True)
    
    def create_results_tab(self):
        """Create results tab."""
        self.results_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.results_frame, text="📋 Results")
        
        # Results display
        results_content = ttk.Frame(self.results_frame)
        results_content.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Results text area
        self.results_text = scrolledtext.ScrolledText(results_content, height=20, width=100)
        self.results_text.pack(fill=tk.BOTH, expand=True)
        
        # Results controls
        controls_frame = ttk.Frame(results_content)
        controls_frame.pack(fill=tk.X, pady=(10, 0))
        
        ttk.Button(controls_frame, text="🔄 Refresh Results",
                  command=self.refresh_results).pack(side=tk.LEFT)
        ttk.Button(controls_frame, text="📄 Export Results",
                  command=self.export_results).pack(side=tk.LEFT, padx=(10, 0))
        ttk.Button(controls_frame, text="🗑️ Clear Results",
                  command=self.clear_results).pack(side=tk.LEFT, padx=(10, 0))
    
    def create_status_bar(self, parent):
        """Create status bar."""
        self.status_frame = ttk.Frame(parent)
        self.status_frame.pack(fill=tk.X, pady=(10, 0))
        
        # Status label
        self.status_var = tk.StringVar(value="Ready")
        status_label = ttk.Label(self.status_frame, textvariable=self.status_var)
        status_label.pack(side=tk.LEFT)
        
        # Connection indicator
        self.connection_indicator = ttk.Label(self.status_frame, text="🔴 Disconnected")
        self.connection_indicator.pack(side=tk.RIGHT)
    
    def setup_logging(self):
        """Setup logging for the GUI."""
        # Configure logging to display in results tab
        self.log_handler = logging.StreamHandler()
        self.log_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        self.log_handler.setFormatter(formatter)
        
        # Add handler to root logger
        logging.getLogger().addHandler(self.log_handler)
    
    # Database connection methods
    def on_driver_change(self, event=None):
        """Handle driver selection change."""
        driver = self.db_driver.get()
        
        if driver == "sqlite":
            self.connection_fields["host"].delete(0, tk.END)
            self.connection_fields["host"].insert(0, "")
            self.connection_fields["port"].delete(0, tk.END)
            self.connection_fields["port"].insert(0, "0")
            self.connection_fields["username"].delete(0, tk.END)
            self.connection_fields["username"].insert(0, "")
            self.connection_fields["password"].delete(0, tk.END)
            self.connection_fields["password"].insert(0, "")

            
            # Disable fields not needed for SQLite
            self.connection_fields["host"].configure(state="disabled")
            self.connection_fields["port"].configure(state="disabled")
            self.connection_fields["username"].configure(state="disabled")
            self.connection_fields["password"].configure(state="disabled")
        
        elif driver == "postgresql":
            # Enable all fields
            for field in self.connection_fields.values():
                field.configure(state="normal")
            
            self.connection_fields["host"].delete(0, tk.END)
            self.connection_fields["host"].insert(0, "localhost")
            self.connection_fields["port"].delete(0, tk.END)
            self.connection_fields["port"].insert(0, "5432")
            self.connection_fields["username"].delete(0, tk.END)
            self.connection_fields["username"].insert(0, "postgres")

        
        elif driver == "mysql":
            # Enable all fields
            for field in self.connection_fields.values():
                field.configure(state="normal")
            
            self.connection_fields["host"].delete(0, tk.END)
            self.connection_fields["host"].insert(0, "localhost")
            self.connection_fields["port"].delete(0, tk.END)
            self.connection_fields["port"].insert(0, "3306")
            self.connection_fields["username"].delete(0, tk.END)
            self.connection_fields["username"].insert(0, "root")

    
    def quick_sqlite(self):
        """Quick SQLite setup."""
        file_path = filedialog.askopenfilename(
            title="Select SQLite Database",
            filetypes=[("SQLite files", "*.db *.sqlite *.sqlite3"), ("All files", "*.*")]
        )
        
        if file_path:
            self.db_driver.set("sqlite")
            self.on_driver_change()
            self.connection_fields["database"].configure(state="normal")
            self.connection_fields["database"].delete(0, tk.END)
            self.connection_fields["database"].insert(0, file_path)
    
    def quick_postgresql(self):
        """Quick PostgreSQL setup."""
        self.db_driver.set("postgresql")
        self.on_driver_change()
    
    def quick_mysql(self):
        """Quick MySQL setup."""
        self.db_driver.set("mysql")
        self.on_driver_change()
    
    def browse_sqlite_file(self):
        """Browse for SQLite database file."""
        filename = filedialog.askopenfilename(
            title="Select SQLite Database File",
            filetypes=[("SQLite files", "*.db *.sqlite *.sqlite3"), ("All files", "*.*")]
        )
        if filename:
            self.sqlite_file_var.set(filename)
    
    def connect_to_server(self):
        """Connect to database server and list available databases."""
        driver = self.db_driver.get()
        
        if driver == "sqlite":
            # For SQLite, go directly to database analysis
            self.connect_to_database()
            return
        
        try:
            # Create server-level connection (no specific database)
            config = DatabaseConfig(
                driver=driver,
                host=self.connection_fields["host"].get(),
                port=int(self.connection_fields["port"].get()),
                database="",  # No specific database for server connection
                username=self.connection_fields["username"].get(),
                password=self.connection_fields["password"].get()
            )
            
            self.status_var.set("Connecting to server...")
            server_conn = DatabaseConnection(config)
            server_conn.connect()
            
            # Get list of databases
            self.status_var.set("Fetching database list...")
            databases = self.get_database_list(server_conn, driver)
            server_conn.close()
            
            if databases:
                # Show database selection
                self.database_combo['values'] = databases
                self.database_combo.set("")
                self.db_select_frame.pack(fill=tk.X, pady=5, after=self.connection_fields["password"].master)
                
                self.connection_status.configure(text="✅ Connected to server", foreground="green")
                self.status_var.set(f"Server connected • {len(databases)} databases found")
                self.analyze_button.configure(state="normal")
                
                messagebox.showinfo("Success", f"Connected to server!\nFound {len(databases)} databases.\nSelect a database to analyze.")
            else:
                raise Exception("No databases found on server")
                
        except Exception as e:
            self.connection_status.configure(text="❌ Server connection failed", foreground="red")
            self.status_var.set("Connection failed")
            messagebox.showerror("Connection Error", f"Failed to connect to server: {str(e)}")
    
    def get_database_list(self, connection, driver):
        """Get list of databases from server."""
        try:
            if driver == "postgresql":
                query = "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname"
            elif driver == "mysql":
                query = "SHOW DATABASES"
            else:
                return []
            
            result = connection.execute_query(query)
            return [row[0] for row in result if row[0] not in ['information_schema', 'performance_schema', 'mysql', 'sys']]
        except Exception as e:
            logger.error(f"Failed to get database list: {e}")
            return []
    
    def on_database_selected(self, event=None):
        """Handle database selection."""
        if self.database_var.get():
            self.analyze_button.configure(state="normal")
    
    def test_connection(self):
        """Test database connection."""
        try:
            config = self.get_db_config()
            
            with DatabaseConnection(config) as db_conn:
                db_conn.connect()
                if db_conn.test_connection():
                    self.connection_status.configure(text="✅ Connection successful", foreground="green")
                    messagebox.showinfo("Success", "Database connection successful!")
                else:
                    self.connection_status.configure(text="❌ Connection failed", foreground="red")
                    messagebox.showerror("Error", "Database connection failed!")
        
        except Exception as e:
            self.connection_status.configure(text="❌ Connection error", foreground="red")
            messagebox.showerror("Connection Error", f"Failed to connect: {str(e)}")
    
    def connect_to_database(self):
        """Connect to database and analyze schema."""
        try:
            config = self.get_db_config()
            
            self.db_connection = DatabaseConnection(config)
            self.db_connection.connect()
            
            # Test connection
            if not self.db_connection.test_connection():
                raise Exception("Connection test failed")
            
            # Analyze schema
            self.status_var.set("Analyzing schema...")
            analyzer = SchemaAnalyzer(self.db_connection)
            self.schema = analyzer.analyze_schema()
            
            # Update GUI
            self.connection_status.configure(text="✅ Connected and analyzed", foreground="green")
            self.connection_indicator.configure(text="🟢 Connected")
            self.status_var.set(f"Connected • {len(self.schema.tables)} tables found")
            
            # Update table configuration
            self.table_panel.update_tables(self.schema)
            
            # Enable generation tab
            self.notebook.tab(2, state="normal")
            
            # Log schema info
            self.log_message(f"Connected to database: {len(self.schema.tables)} tables found")
            for table in self.schema.tables:
                self.log_message(f"  - {table.name}: {len(table.columns)} columns")
            
            messagebox.showinfo("Success", f"Connected successfully!\nFound {len(self.schema.tables)} tables.")
        
        except Exception as e:
            self.connection_status.configure(text="❌ Connection failed", foreground="red")
            self.connection_indicator.configure(text="🔴 Disconnected")
            messagebox.showerror("Connection Error", f"Failed to connect: {str(e)}")
    
    def get_db_config(self) -> DatabaseConfig:
        """Get database configuration from form."""
        driver = self.db_driver.get()
        
        if driver == "sqlite":
            return DatabaseConfig(
                host="",
                port=0,
                database=self.sqlite_file_var.get(),
                username="",
                password="",
                driver=driver
            )
        else:
            database = self.database_var.get() if hasattr(self, 'database_var') else ""
            return DatabaseConfig(
                host=self.connection_fields["host"].get(),
                port=int(self.connection_fields["port"].get() or "0"),
                database=database,
                username=self.connection_fields["username"].get(),
                password=self.connection_fields["password"].get(),
                driver=driver
            )
    
    # Generation methods
    def start_generation(self):
        """Start data generation."""
        if not self.db_connection or not self.schema:
            messagebox.showerror("Error", "Please connect to a database first!")
            return
        
        # Get configuration
        try:
            config = self.build_generation_config()
            table_configs = self.table_panel.get_table_configs()
            
            # Filter tables with row count > 0
            tables_to_generate = {name: count for name, count in table_configs.items() if count > 0}
            
            if not tables_to_generate:
                messagebox.showwarning("Warning", "No tables selected for generation!")
                return
            
            total_rows = sum(tables_to_generate.values())
            
            # Confirm generation
            if total_rows > 100000:
                result = messagebox.askyesno(
                    "Confirm Generation",
                    f"You are about to generate {total_rows:,} rows.\n"
                    f"This may take some time. Continue?"
                )
                if not result:
                    return
            
            # Start generation in thread
            self.stop_generation_flag.clear()
            self.generate_button.configure(state=tk.DISABLED)
            self.stop_button.configure(state=tk.NORMAL)
            
            # Start progress monitoring
            self.progress_monitor.start_generation(total_rows)
            
            # Start generation thread
            thread = threading.Thread(
                target=self.run_generation,
                args=(config, tables_to_generate),
                daemon=True
            )
            thread.start()
            
        except Exception as e:
            messagebox.showerror("Configuration Error", f"Failed to start generation: {str(e)}")
    
    def build_generation_config(self) -> EnhancedGenerationConfig:
        """Build generation configuration from GUI settings."""
        # Get performance settings
        performance_config = self.performance_panel.get_performance_config()
        
        # Get duplicate settings
        duplicate_config = self.duplicate_panel.get_duplicate_config()
        
        # Build enhanced config
        config = EnhancedGenerationConfig(
            performance=performance_config,
            duplicates=duplicate_config,
            generation_mode=self.generation_mode.get(),
            truncate_existing=self.truncate_tables.get(),
            seed=self.seed_value.get() if self.use_seed.get() else None
        )
        
        return config
    
    def run_generation(self, config: EnhancedGenerationConfig, table_configs: Dict[str, int]):
        """Run data generation in background thread."""
        try:
            total_start_time = time.time()
            total_generated = 0
            
            # Choose generator based on mode
            generation_mode = self.generation_mode.get()
            
            if generation_mode == "ultra_high_performance":
                # Ultra-fast processor
                self.log_message("🚀 Starting ultra-fast processing...")
                processor = create_ultra_fast_processor(self.schema, config, self.db_connection)
                self.current_generator = processor
                
                for table_name, row_count in table_configs.items():
                    if self.stop_generation_flag.is_set():
                        break
                    
                    self.log_message(f"Processing {table_name}: {row_count:,} rows")
                    
                    # Progress callback
                    def progress_callback(table, current, total):
                        rate = current / (time.time() - total_start_time) if time.time() > total_start_time else 0
                        self.update_progress(total_generated + current, table, rate)
                        self.system_monitor.update_performance(rate, total_generated + current)
                    
                    # Generate data
                    report = processor.process_millions_of_records(
                        table_name, row_count, progress_callback
                    )
                    
                    total_generated += report.total_rows_generated
                    self.log_message(f"✅ {table_name}: {report.total_rows_generated:,} rows generated")
            
            elif generation_mode == "bulk":
                # High-performance generator
                self.log_message("📈 Starting high-performance generation...")
                generator = HighPerformanceGenerator(self.schema, config, self.db_connection)
                self.current_generator = generator
                
                for table_name, row_count in table_configs.items():
                    if self.stop_generation_flag.is_set():
                        break
                    
                    self.log_message(f"Generating {table_name}: {row_count:,} rows")
                    
                    # Progress callback
                    def progress_callback(table, current, total):
                        rate = current / (time.time() - total_start_time) if time.time() > total_start_time else 0
                        self.update_progress(total_generated + current, table, rate)
                        self.system_monitor.update_performance(rate, total_generated + current)
                    
                    # Generate data
                    stats = generator.generate_millions_of_records(
                        table_name, row_count, progress_callback, self.use_streaming.get()
                    )
                    
                    total_generated += stats.total_rows_generated
                    self.log_message(f"✅ {table_name}: {stats.total_rows_generated:,} rows generated")
            
            else:
                # Standard generation - use appropriate processor based on config
                total_rows = sum(table_configs.values())
                
                # Auto-upgrade to ultra-fast if fast data reuse is enabled or large dataset
                if (config.duplicates.enable_fast_data_reuse or 
                    config.duplicates.global_duplicate_strategy == DuplicateStrategy.FAST_DATA_REUSE or
                    total_rows >= 100000):
                    
                    self.log_message("🚀 Auto-upgrading to ultra-fast processing...")
                    processor = create_ultra_fast_processor(self.schema, config, self.db_connection)
                    self.current_generator = processor
                    
                    for table_name, row_count in table_configs.items():
                        if self.stop_generation_flag.is_set():
                            break
                        
                        self.log_message(f"Processing {table_name}: {row_count:,} rows")
                        
                        # Progress callback
                        def progress_callback(table, current, total):
                            rate = current / (time.time() - total_start_time) if time.time() > total_start_time else 0
                            self.update_progress(total_generated + current, table, rate)
                            self.system_monitor.update_performance(rate, total_generated + current)
                        
                        # Generate data
                        report = processor.process_millions_of_records(
                            table_name, row_count, progress_callback
                        )
                        
                        total_generated += report.total_rows_generated
                        self.log_message(f"✅ {table_name}: {report.total_rows_generated:,} rows generated")
                
                else:
                    # Use high-performance generator for smaller datasets
                    self.log_message("📈 Starting high-performance generation...")
                    generator = HighPerformanceGenerator(self.schema, config, self.db_connection)
                    self.current_generator = generator
                    
                    for table_name, row_count in table_configs.items():
                        if self.stop_generation_flag.is_set():
                            break
                        
                        self.log_message(f"Generating {table_name}: {row_count:,} rows")
                        
                        # Progress callback
                        def progress_callback(table, current, total):
                            rate = current / (time.time() - total_start_time) if time.time() > total_start_time else 0
                            self.update_progress(total_generated + current, table, rate)
                            self.system_monitor.update_performance(rate, total_generated + current)
                        
                        # Generate data
                        stats = generator.generate_millions_of_records(
                            table_name, row_count, use_streaming=self.use_streaming.get()
                        )
                        
                        total_generated += stats.get('rows_generated', row_count)
                        self.log_message(f"✅ {table_name}: {stats.get('rows_generated', row_count):,} rows generated")
            
            # Complete
            total_time = time.time() - total_start_time
            avg_rate = total_generated / total_time if total_time > 0 else 0
            
            self.log_message(f"🎉 Generation completed!")
            self.log_message(f"📊 Total: {total_generated:,} rows in {total_time:.2f}s ({avg_rate:,.0f} rows/s)")
            
            # Update progress monitor
            self.progress_monitor.complete(total_time, total_generated)
            
            # Show completion message
            completion_message = (
                f"Successfully generated {total_generated:,} rows in {total_time:.2f} seconds!\n"
                f"Average rate: {avg_rate:,.0f} rows/second"
            )
            self.root.after(0, lambda msg=completion_message: messagebox.showinfo("Generation Complete", msg))
        
        except Exception as e:
            error_message = f"Generation failed: {str(e)}"
            self.log_message(f"❌ {error_message}")
            self.root.after(0, lambda msg=error_message: messagebox.showerror("Generation Error", msg))
        
        finally:
            # Reset UI
            self.root.after(0, self.generation_complete)
    
    def stop_generation(self):
        """Stop data generation."""
        self.stop_generation_flag.set()
        self.log_message("🛑 Stopping generation...")
        
        if hasattr(self.current_generator, 'cleanup'):
            self.current_generator.cleanup()
        
        self.generation_complete()
    
    def generation_complete(self):
        """Handle generation completion."""
        self.generate_button.configure(state=tk.NORMAL)
        self.stop_button.configure(state=tk.DISABLED)
        self.status_var.set("Generation completed")
    
    def on_duplicate_strategy_change(self):
        """Handle duplicate strategy change to suggest optimal settings."""
        strategy = self.duplicate_panel.duplicate_strategy.get()
        
        if strategy == "fast_data_reuse":
            # Suggest ultra-fast mode for best performance
            current_mode = self.generation_mode.get()
            if current_mode != "ultra_high_performance":
                result = messagebox.askyesno(
                    "Optimization Suggestion",
                    "Fast Data Reuse works best with Ultra-Fast Processing mode.\n\n"
                    "Would you like to switch to Ultra-Fast Processing for optimal performance?"
                )
                if result:
                    self.generation_mode.set("ultra_high_performance")
                    self.log_message("🚀 Switched to Ultra-Fast Processing mode for optimal fast data reuse")
    
    def update_progress(self, current_rows: int, current_table: str, rate: float):
        """Update progress in main thread."""
        self.root.after(0, lambda: self.progress_monitor.update_progress(current_rows, current_table, rate))
    
    # Configuration methods
    def save_configuration(self):
        """Save current configuration to file."""
        try:
            config = self.build_generation_config()
            table_configs = self.table_panel.get_table_configs()
            
            config_data = {
                'generation_config': config.dict(),
                'table_configs': table_configs,
                'database_config': {
                    'driver': self.db_driver.get(),
                    'host': self.connection_fields['host'].get(),
                    'port': self.connection_fields['port'].get(),
                    'database': self.connection_fields['database'].get(),
                    'username': self.connection_fields['username'].get()
                    # Note: Password not saved for security
                }
            }
            
            file_path = filedialog.asksaveasfilename(
                title="Save Configuration",
                filetypes=[("YAML files", "*.yaml *.yml"), ("JSON files", "*.json")],
                defaultextension=".yaml"
            )
            
            if file_path:
                if file_path.endswith('.json'):
                    with open(file_path, 'w') as f:
                        json.dump(config_data, f, indent=2, default=str)
                else:
                    with open(file_path, 'w') as f:
                        yaml.dump(config_data, f, default_flow_style=False)
                
                messagebox.showinfo("Success", f"Configuration saved to {file_path}")
        
        except Exception as e:
            messagebox.showerror("Save Error", f"Failed to save configuration: {str(e)}")
    
    def load_configuration(self):
        """Load configuration from file."""
        try:
            file_path = filedialog.askopenfilename(
                title="Load Configuration",
                filetypes=[("Config files", "*.yaml *.yml *.json"), ("All files", "*.*")]
            )
            
            if file_path:
                if file_path.endswith('.json'):
                    with open(file_path, 'r') as f:
                        config_data = json.load(f)
                else:
                    with open(file_path, 'r') as f:
                        config_data = yaml.safe_load(f)
                
                # Apply configuration to GUI
                self.apply_configuration(config_data)
                
                messagebox.showinfo("Success", f"Configuration loaded from {file_path}")
        
        except Exception as e:
            messagebox.showerror("Load Error", f"Failed to load configuration: {str(e)}")
    
    def apply_configuration(self, config_data: Dict[str, Any]):
        """Apply configuration data to GUI."""
        # Apply database config
        if 'database_config' in config_data:
            db_config = config_data['database_config']
            self.db_driver.set(db_config.get('driver', 'sqlite'))
            self.on_driver_change()
            
            for key, field in self.connection_fields.items():
                if key in db_config:
                    field.delete(0, tk.END)
                    field.insert(0, str(db_config[key]))
        
        # Apply generation config
        if 'generation_config' in config_data:
            gen_config = config_data['generation_config']
            
            # Performance settings
            if 'performance' in gen_config:
                perf = gen_config['performance']
                self.performance_panel.performance_mode.set(perf.get('performance_mode', 'balanced'))
                self.performance_panel.max_workers.set(perf.get('max_workers', 4))
                self.performance_panel.batch_size.set(perf.get('batch_size', 10000))
                self.performance_panel.enable_multiprocessing.set(perf.get('enable_multiprocessing', False))
                self.performance_panel.cache_size.set(perf.get('cache_size_mb', 500))
            
            # Duplicate settings
            if 'duplicates' in gen_config:
                dup = gen_config['duplicates']
                self.duplicate_panel.enable_duplicates.set(dup.get('global_duplicate_enabled', False))
                self.duplicate_panel.duplicate_strategy.set(dup.get('global_duplicate_strategy', 'smart_duplicates'))
                self.duplicate_panel.duplicate_probability.set(dup.get('global_duplicate_probability', 0.3))
                self.duplicate_panel.toggle_duplicate_options()
        
        # Apply table configs
        if 'table_configs' in config_data and self.schema:
            table_configs = config_data['table_configs']
            for table_name, row_count in table_configs.items():
                if table_name in self.table_panel.table_configs:
                    self.table_panel.table_configs[table_name].set(row_count)
    
    # Utility methods
    def log_message(self, message: str):
        """Log message to results tab."""
        timestamp = time.strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        self.root.after(0, lambda entry=log_entry: self.results_text.insert(tk.END, entry))
        self.root.after(0, lambda: self.results_text.see(tk.END))
    
    def refresh_results(self):
        """Refresh results display."""
        # This would query the database for current row counts
        pass
    
    def export_results(self):
        """Export generation results."""
        try:
            file_path = filedialog.asksaveasfilename(
                title="Export Results",
                filetypes=[("Text files", "*.txt"), ("CSV files", "*.csv")],
                defaultextension=".txt"
            )
            
            if file_path:
                content = self.results_text.get(1.0, tk.END)
                with open(file_path, 'w') as f:
                    f.write(content)
                
                messagebox.showinfo("Success", f"Results exported to {file_path}")
        
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export results: {str(e)}")
    
    def clear_results(self):
        """Clear results display."""
        self.results_text.delete(1.0, tk.END)
    
    def process_results(self):
        """Process results from background threads."""
        try:
            while True:
                message = self.result_queue.get_nowait()
                self.log_message(message)
        except queue.Empty:
            pass
        
        # Schedule next check
        self.root.after(100, self.process_results)


def main():
    """Main function to run the enhanced GUI."""
    root = tk.Tk()
    app = EnhancedDBMockerGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
