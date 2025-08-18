#!/usr/bin/env python3
"""
Launch script for the Enhanced DBMocker GUI
"""

import sys
import os
import tkinter as tk
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def check_dependencies():
    """Check if all required dependencies are available."""
    required_modules = [
        'tkinter',
        'psutil',
        'sqlalchemy',
        'yaml',
        'numpy'
    ]
    
    missing_modules = []
    
    for module in required_modules:
        try:
            __import__(module)
        except ImportError:
            missing_modules.append(module)
    
    if missing_modules:
        print("❌ Missing required dependencies:")
        for module in missing_modules:
            print(f"   - {module}")
        print("\n💡 Install missing dependencies with:")
        print(f"   pip install {' '.join(missing_modules)}")
        return False
    
    return True

def main():
    """Main function to launch the enhanced GUI."""
    print("🚀 Starting DBMocker Ultra GUI...")
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    try:
        # Import and run the enhanced GUI
        from dbmocker.gui.enhanced_main import EnhancedDBMockerGUI
        
        # Create root window
        root = tk.Tk()
        
        # Create and run the application
        app = EnhancedDBMockerGUI(root)
        
        print("✅ GUI launched successfully!")
        print("📋 Features available:")
        print("   • Ultra-fast data generation for millions of records")
        print("   • Advanced performance configuration")
        print("   • Smart duplicate handling strategies")
        print("   • Real-time system monitoring")
        print("   • Multi-threading and connection pooling")
        print("   • Streaming mode for memory efficiency")
        
        # Start the GUI
        root.mainloop()
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("💡 Make sure all required modules are installed and accessible")
        sys.exit(1)
    
    except Exception as e:
        print(f"❌ Error launching GUI: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
