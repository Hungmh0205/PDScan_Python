#!/usr/bin/env python3
"""
Script to run PDScan API server
"""

import argparse
import sys
import os
import traceback

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def main():
    try:
        print("🔧 Loading PDScan API...")
        from pdscan.api import run_api_server
        print("✅ API loaded successfully")
        
        parser = argparse.ArgumentParser(description="Run PDScan API Server")
        parser.add_argument("--host", default="0.0.0.0", help="Host to bind to (default: 0.0.0.0)")
        parser.add_argument("--port", type=int, default=8000, help="Port to bind to (default: 8000)")
        parser.add_argument("--debug", action="store_true", help="Enable debug mode")
        parser.add_argument("--reload", action="store_true", help="Enable auto-reload on code changes")
        
        args = parser.parse_args()
        
        print("🚀 Starting PDScan API Server...")
        print(f"📍 Host: {args.host}")
        print(f"🔌 Port: {args.port}")
        print(f"🐛 Debug: {args.debug}")
        print(f"🔄 Auto-reload: {args.reload}")
        print()
        print("📚 API Documentation:")
        print(f"   Swagger UI: http://{args.host}:{args.port}/api/docs")
        print(f"   ReDoc: http://{args.host}:{args.port}/api/redoc")
        print(f"   Health Check: http://{args.host}:{args.port}/api/v1/health")
        print()
        print("🔑 Authentication:")
        print("   Use API key in Authorization header: Bearer <your-api-key>")
        print("   Or as query parameter: ?api_key=<your-api-key>")
        print()
        print("Press Ctrl+C to stop the server")
        print("=" * 60)
        
        run_api_server(
            host=args.host,
            port=args.port,
            debug=args.debug
        )
        
    except KeyboardInterrupt:
        print("\n🛑 Server stopped by user")
    except ImportError as e:
        print(f"\n❌ Import error: {e}")
        print("Make sure all dependencies are installed:")
        print("pip install -r requirements.txt")
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Server error: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 