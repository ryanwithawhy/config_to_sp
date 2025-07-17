#!/usr/bin/env python3
"""
Test script to verify mongosh command construction for stream processor creation.
"""

import json
import subprocess
import sys

def test_mongosh_command_construction():
    """Test that the mongosh command is constructed correctly."""
    
    # Test data
    stream_processor_url = "mongodb://atlas-stream-67d97a1d8820c02d373b6f28-gkmrk.virginia-usa.a.query.mongodb.net/"
    mongodb_username = "ryan"
    mongodb_password = "test_password"
    stream_processor_name = "test1_hackathon_hack1"
    
    # Create test pipeline
    pipeline = [
        {
            "$source": {
                "connectionName": "mongodb-source-connection",
                "db": "hackathon",
                "coll": "hack1"
            }
        },
        {
            "$emit": {
                "connectionName": "kafka-connection-name",
                "topic": "test3.hackathon.hack1"
            }
        }
    ]
    
    # Ensure URL ends with exactly one slash
    if not stream_processor_url.endswith('/'):
        stream_processor_url += '/'
    
    # Create JavaScript command for mongosh
    pipeline_json = json.dumps(pipeline)
    js_command = f'sp.createStreamProcessor("{stream_processor_name}", {pipeline_json})'
    
    # Build mongosh command (no quotes around URI when using subprocess)
    mongosh_cmd = [
        'mongosh',
        stream_processor_url,
        '--tls',
        '--authenticationDatabase', 'admin',
        '--username', mongodb_username,
        '--password', mongodb_password,
        '--eval', js_command
    ]
    
    print("=" * 60)
    print("MONGOSH COMMAND CONSTRUCTION TEST")
    print("=" * 60)
    
    print(f"Stream Processor URL: {stream_processor_url}")
    print(f"Username: {mongodb_username}")
    print(f"Stream Processor Name: {stream_processor_name}")
    print()
    
    print("JavaScript Command:")
    print(js_command)
    print()
    
    print("Full mongosh command as list:")
    for i, arg in enumerate(mongosh_cmd):
        print(f"  [{i}]: {repr(arg)}")
    print()
    
    print("Command as shell string (for reference):")
    # Show what it would look like as a shell command (for debugging)
    shell_cmd = ' '.join([f'"{arg}"' if ' ' in arg else arg for arg in mongosh_cmd])
    print(f"  {shell_cmd}")
    print()
    
    print("Testing command execution (dry run - will fail auth but shows URI parsing):")
    try:
        # Run with a short timeout to see if URI parsing works
        result = subprocess.run(mongosh_cmd, capture_output=True, text=True, timeout=10)
        
        print(f"Return code: {result.returncode}")
        if result.stdout:
            print("STDOUT:")
            print(result.stdout)
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
            
        # Check for specific URI errors
        if "Invalid URI" in result.stderr:
            print("❌ URI PARSING FAILED - Invalid URI error detected")
        elif "authentication failed" in result.stderr.lower() or "unauthorized" in result.stderr.lower():
            print("✅ URI PARSING OK - Authentication error (expected with test credentials)")
        elif result.returncode == 0:
            print("✅ COMMAND EXECUTED SUCCESSFULLY")
        else:
            print(f"⚠️  UNKNOWN ERROR - Return code: {result.returncode}")
            
    except subprocess.TimeoutExpired:
        print("⏱️  Command timed out (expected - likely waiting for auth)")
    except FileNotFoundError:
        print("❌ mongosh not found in PATH")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    
    print("=" * 60)

if __name__ == "__main__":
    test_mongosh_command_construction()