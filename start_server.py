import argparse
import uvicorn
from src.main import create_app

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-data-load", action="store_true", help="Skip initial data load during startup")
    parser.add_argument("--port", type=int, default=8000, help="Port to run the server on")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host to run the server on")
    args = parser.parse_args()

    # Create app with appropriate skip_data_load setting
    app = create_app(skip_data_load=args.skip_data_load)
    
    # Run the server
    uvicorn.run(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main() 
