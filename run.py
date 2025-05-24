import uvicorn
import os
import sys
from pathlib import Path

if __name__ == "__main__":
    # Get the absolute path to the project root
    project_root = Path(__file__).parent.absolute()
    
    # Add project root to Python path
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    # Run the FastAPI application
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=1
    ) 
