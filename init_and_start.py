import os
import sys
import subprocess
from src.modules.rrg.init import init_database
from src.utils.logger import get_logger

logger = get_logger(__name__)

def main():
    try:
        # First, ensure no other Python processes are running
        if os.name == 'nt':  # Windows
            subprocess.run(['taskkill', '/F', '/IM', 'python.exe'], capture_output=True)
        
        # Initialize the database
        logger.info("Starting database initialization...")
        success = init_database()
        
        if not success:
            logger.error("Database initialization failed!")
            sys.exit(1)
            
        logger.info("Database initialized successfully!")
        
        # Start the server with skip-data-load flag
        logger.info("Starting server...")
        subprocess.run([sys.executable, 'start_server.py', '--skip-data-load'])
        
    except Exception as e:
        logger.error(f"Error during initialization: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 
