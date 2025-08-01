#!/usr/bin/env python3
"""
End-to-end integration tests that create real stream processors and verify they work.

This test:
1. Scans integration_configs/ for test config files
2. Creates complete configs by merging with env variables
3. Runs create_processors.py to create actual stream processors
4. Uses mongosh to verify processors exist with sp.<name>.stats()
5. Cleans up by deleting test processors

Test config files should be minimal and only contain:
- connector.class
- name (will be used for collection name too)
- The specific fields being tested

All other fields (credentials, topics, database, etc.) come from env variables.
"""

import os
import sys
import unittest
import tempfile
import json
import subprocess
import shutil
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Load integration-specific .env file
load_dotenv(Path(__file__).parent / '.env.integration')


class TestE2EIntegration(unittest.TestCase):
    """End-to-end integration tests using real stream processors."""
    
    @classmethod
    def setUpClass(cls):
        """Set up class-level test fixtures."""
        # Audit mode is set by main section before unittest runs
        if not hasattr(cls, 'audit_mode'):
            cls.audit_mode = False
            cls.audit_dir = None
        # Ensure Atlas authentication (silently)
        try:
            subprocess.run(['atlas', 'auth', 'login'], capture_output=True, text=True)
        except:
            pass
        
        # Check required environment variables
        cls.required_env_vars = [
            'test_kafka_api_key', 'test_kafka_api_secret', 'test_db_user',
            'test_db_password', 'test_confluent_cluster_id', 'test_confluent_rest_endpoint',
            'test_stream_processor_url', 'test_tenant_name', 'test_group_id',
            'test_cluster_name', 'test_topic', 'test_database'
        ]
        
        missing_vars = [var for var in cls.required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise unittest.SkipTest(
                f"Missing required environment variables: {', '.join(missing_vars)}\n"
                f"Please set all test_* environment variables before running integration tests."
            )
        
        print("✅ Environment variables verified")
        
        # Store created processors for cleanup
        cls.created_processors = []
    
    @classmethod
    def tearDownClass(cls):
        """Clean up any processors created during tests."""
        if cls.created_processors:
            print(f"\n🧹 Cleaning up {len(cls.created_processors)} test processors...")
            for processor_name in cls.created_processors:
                try:
                    cls.delete_processor(processor_name)
                    print(f"   Deleted: {processor_name}")
                except Exception as e:
                    print(f"   Failed to delete {processor_name}: {e}")
    
    def setUp(self):
        """Set up test fixtures before each test."""
        # Create temporary directory for this test
        self.temp_dir = tempfile.mkdtemp(prefix="e2e_integration_test_")
        self.config_dir = Path(self.temp_dir) / "configs"
        self.config_dir.mkdir()
        
        # Create main config using environment variables
        self.main_config = {
            "confluent-cluster-id": os.getenv('test_confluent_cluster_id'),
            "confluent-rest-endpoint": os.getenv('test_confluent_rest_endpoint'),
            "mongodb-stream-processor-instance-url": os.getenv('test_stream_processor_url'),
            "mongodb-tenant-name": os.getenv('test_tenant_name'),
            "mongodb-group-id": os.getenv('test_group_id'),
            "kafka-connection-name": os.getenv('test_kafka_connection_name'),
            "mongodb-cluster-name": os.getenv('test_cluster_name'),
            "mongodb-connection-name": os.getenv('test_mongodb_connection_name'),
            "mongodb-connection-role": os.getenv('test_mongodb_connection_role')
        }
        
        self.main_config_path = Path(self.temp_dir) / "main.json"
        with open(self.main_config_path, 'w') as f:
            json.dump(self.main_config, f, indent=2)
    
    def tearDown(self):
        """Clean up test fixtures after each test."""
        try:
            shutil.rmtree(self.temp_dir)
        except Exception as e:
            print(f"Warning: Failed to clean up temp directory: {e}")
    
    def create_full_config(self, test_config: dict) -> dict:
        """Create a full config by merging test config with env variables."""
        connector_class = test_config["connector.class"]
        name = test_config["name"]
        
        # Base config with all required fields from env
        full_config = {
            "connector.class": connector_class,
            "name": name,
            "kafka.auth.mode": "KAFKA_API_KEY",
            "kafka.api.key": os.getenv('test_kafka_api_key'),
            "kafka.api.secret": os.getenv('test_kafka_api_secret'),
            "connection.user": os.getenv('test_db_user'),
            "connection.password": os.getenv('test_db_password'),
            "database": os.getenv('test_database'),
            "collection": name  # Use processor name as collection name
        }
        
        # Add connector-specific required fields
        if "Source" in connector_class:
            full_config["topic.prefix"] = os.getenv('test_topic')
        elif "Sink" in connector_class:
            full_config["topics"] = os.getenv('test_topic')
        
        # Merge in the test-specific fields (overriding defaults)
        full_config.update(test_config)
        
        return full_config
    
    def run_create_processors(self) -> subprocess.CompletedProcess:
        """Run create_processors.py and return the result."""
        cmd = [
            sys.executable,
            str(project_root / "create_processors.py"),
            str(self.main_config_path),
            str(self.config_dir)
        ]
        
        return subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    
    def check_processor_exists(self, processor_name: str) -> bool:
        """Check if processor exists using mongosh sp.<name>.stats()."""
        try:
            # Use authentication like in common.py
            stream_processor_url = os.getenv('test_stream_processor_url')
            if not stream_processor_url.endswith('/'):
                stream_processor_url += '/'
                
            cmd = [
                "mongosh", 
                stream_processor_url,
                "--tls",
                "--authenticationDatabase", "admin",
                "--username", os.getenv('test_db_user'),
                "--password", os.getenv('test_db_password'),
                "--eval", f"sp['{processor_name}'].stats()"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            # Check if running in verbose mode
            import sys
            verbose_mode = '-v' in sys.argv or '--verbose' in sys.argv
            
            if verbose_mode and result.returncode == 0:
                print(f"\n📊 Stats for processor '{processor_name}':")
                print("=" * 50)
                print(result.stdout)
                if result.stderr:
                    print("STDERR:")
                    print(result.stderr)
                print("=" * 50)
            
            # If stats() succeeds, processor exists
            return result.returncode == 0 and "stats" in result.stdout.lower()
            
        except Exception as e:
            print(f"Error checking processor {processor_name}: {e}")
            return False
    
    @classmethod
    def delete_processor(cls, processor_name: str):
        """Delete a processor using mongosh."""
        # Use authentication like in common.py
        stream_processor_url = os.getenv('test_stream_processor_url')
        if not stream_processor_url.endswith('/'):
            stream_processor_url += '/'
            
        cmd = [
            "mongosh",
            stream_processor_url,
            "--tls",
            "--authenticationDatabase", "admin",
            "--username", os.getenv('test_db_user'),
            "--password", os.getenv('test_db_password'),
            "--eval", f"sp['{processor_name}'].drop()"
        ]
        
        subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    
    def audit_processor_stats(self, processor_name: str, test_config: dict, full_config: dict):
        """Capture processor stats and save to audit directory with configs."""
        if not self.__class__.audit_mode:
            return
            
        # Create processor-specific directory
        processor_dir = self.__class__.audit_dir / processor_name
        processor_dir.mkdir(exist_ok=True)
        
        # Save initial test config
        test_config_file = processor_dir / "initial_test_config.json"
        with open(test_config_file, 'w') as f:
            json.dump(test_config, f, indent=2)
        
        # Save full generated config (what customer would provide)
        full_config_file = processor_dir / "full_customer_config.json"
        with open(full_config_file, 'w') as f:
            json.dump(full_config, f, indent=2)
        
        # Capture stats
        stats_file = processor_dir / "stats_output.txt"
        try:
            # Use authentication like in common.py
            stream_processor_url = os.getenv('test_stream_processor_url')
            if not stream_processor_url.endswith('/'):
                stream_processor_url += '/'
                
            cmd = [
                "mongosh", 
                stream_processor_url,
                "--tls",
                "--authenticationDatabase", "admin",
                "--username", os.getenv('test_db_user'),
                "--password", os.getenv('test_db_password'),
                "--eval", f"sp['{processor_name}'].stats()"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            # Write stats (success or failure) to file
            with open(stats_file, 'w') as f:
                f.write(f"Processor: {processor_name}\n")
                f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                f.write(f"Command: {' '.join(cmd)}\n")
                f.write(f"Return Code: {result.returncode}\n")
                f.write(f"{'='*60}\n\n")
                
                if result.returncode == 0:
                    f.write("✅ STATS CAPTURED SUCCESSFULLY\n\n")
                    f.write("STDOUT:\n")
                    f.write(result.stdout)
                    if result.stderr:
                        f.write("\n\nSTDERR:\n")
                        f.write(result.stderr)
                else:
                    f.write("❌ ERROR CAPTURING STATS\n\n")
                    f.write("STDOUT:\n")
                    f.write(result.stdout or "(empty)")
                    f.write("\n\nSTDERR:\n")
                    f.write(result.stderr or "(empty)")
            
            if result.returncode == 0:
                print(f"📊 Audit data saved for '{processor_name}' to {processor_dir}")
            else:
                print(f"⚠️  Stats capture failed for '{processor_name}', error logged to {processor_dir}")
                
        except Exception as e:
            # Write exception to stats file
            with open(stats_file, 'w') as f:
                f.write(f"Processor: {processor_name}\n")
                f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                f.write(f"{'='*60}\n\n")
                f.write("❌ EXCEPTION DURING STATS CAPTURE\n\n")
                f.write(f"Exception: {str(e)}\n")
                f.write(f"Exception Type: {type(e).__name__}\n")
            
            print(f"⚠️  Exception during audit for '{processor_name}', logged to {processor_dir}: {e}")
    
    def audit_failed_processor(self, processor_name: str, test_config: dict, full_config: dict, create_result: subprocess.CompletedProcess):
        """Capture audit data for failed processor creation."""
        if not self.__class__.audit_mode:
            return
            
        # Create processor-specific directory
        processor_dir = self.__class__.audit_dir / processor_name
        processor_dir.mkdir(exist_ok=True)
        
        # Save initial test config
        test_config_file = processor_dir / "initial_test_config.json"
        with open(test_config_file, 'w') as f:
            json.dump(test_config, f, indent=2)
        
        # Save full generated config (what customer would provide)
        full_config_file = processor_dir / "full_customer_config.json"
        with open(full_config_file, 'w') as f:
            json.dump(full_config, f, indent=2)
        
        # Save creation failure details
        failure_file = processor_dir / "creation_failure.txt"
        with open(failure_file, 'w') as f:
            f.write(f"Processor: {processor_name}\n")
            f.write(f"Timestamp: {datetime.now().isoformat()}\n")
            f.write(f"Status: PROCESSOR CREATION FAILED\n")
            f.write(f"Return Code: {create_result.returncode}\n")
            f.write(f"{'='*60}\n\n")
            f.write("❌ PROCESSOR CREATION FAILED\n\n")
            f.write("CREATE_PROCESSORS.PY STDOUT:\n")
            f.write(create_result.stdout or "(empty)")
            f.write("\n\nCREATE_PROCESSORS.PY STDERR:\n")
            f.write(create_result.stderr or "(empty)")
        
        print(f"📋 Audit data saved for failed processor '{processor_name}' to {processor_dir}")
    
    def run_integration_test_for_config(self, config_file: Path):
        """Run an integration test for a specific config file."""
        print(f"\n{'='*80}")
        print(f"TESTING CONFIG: {config_file.name}")
        print(f"{'='*80}")
        
        # Load the test config
        with open(config_file, 'r') as f:
            test_config = json.load(f)
        
        print(f"Test config: {json.dumps(test_config, indent=2)}")
        
        # Create full config
        full_config = self.create_full_config(test_config)
        processor_name = full_config["name"]
        
        print(f"Processor name: {processor_name}")
        
        # Write full config to temp file
        temp_config_path = self.config_dir / f"{config_file.stem}.json"
        with open(temp_config_path, 'w') as f:
            json.dump(full_config, f, indent=2)
        
        # Run create_processors
        print("Running create_processors.py...")
        result = self.run_create_processors()
        
        print("STDOUT:")
        print(result.stdout)
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        
        # Verify basic success
        self.assertIn("Main config loaded successfully", result.stdout,
                     "Main config should load successfully")
        self.assertIn("PROCESSING COMPLETE", result.stdout,
                     "Processing should complete")
        
        # Check if processor was created
        print(f"Checking if processor '{processor_name}' exists...")
        processor_exists = self.check_processor_exists(processor_name)
        
        if processor_exists:
            print(f"✅ Processor '{processor_name}' created successfully!")
            # Capture audit stats before adding to cleanup list
            self.audit_processor_stats(processor_name, test_config, full_config)
            # Add to cleanup list
            self.__class__.created_processors.append(processor_name)
        else:
            # Still capture audit data for failed processors
            self.audit_failed_processor(processor_name, test_config, full_config, result)
            self.fail(f"Processor '{processor_name}' was not created. Check output above.")
        
        return processor_name
    
    def test_all_integration_configs(self):
        """Discover and test all config files in integration_configs/."""
        integration_configs_dir = Path(__file__).parent / "integration_configs"
        
        if not integration_configs_dir.exists():
            self.skipTest("No integration_configs directory found")
        
        config_files = list(integration_configs_dir.glob("*.json"))
        
        if not config_files:
            self.skipTest("No config files found in integration_configs/")
        
        print(f"Found {len(config_files)} config files to test:")
        for config_file in config_files:
            print(f"  - {config_file.name}")
        
        # Test each config file
        for config_file in config_files:
            with self.subTest(config=config_file.name):
                self.run_integration_test_for_config(config_file)


if __name__ == '__main__':
    import sys
    import os
    
    # Handle custom flags before unittest processes arguments
    if '-dlq' in sys.argv:
        os.environ['DEBUG_DLQ'] = 'true'
        sys.argv.remove('-dlq')
    
    # Handle custom --audit flag before unittest processes arguments
    if '--audit' in sys.argv:
        sys.argv.remove('--audit')
        # Set up audit mode on the test class
        TestE2EIntegration.audit_mode = True
        # Create audit directory with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        audit_base_dir = Path(__file__).parent / "audit_results"
        audit_base_dir.mkdir(exist_ok=True)
        TestE2EIntegration.audit_dir = audit_base_dir / timestamp
        TestE2EIntegration.audit_dir.mkdir(parents=True, exist_ok=True)
        print(f"📋 Audit mode enabled. Results will be saved to: {TestE2EIntegration.audit_dir}")
    else:
        TestE2EIntegration.audit_mode = False
        TestE2EIntegration.audit_dir = None
    
    unittest.main(verbosity=2)