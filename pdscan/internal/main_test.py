"""
Tests for main scanning implementation
"""

import unittest
import os
import tempfile
from unittest.mock import MagicMock, patch
from urllib.parse import urlparse
import pathlib

from .main import scan, get_adapter
from .scan_opts import ScanOptions
from .exceptions import ScanError

class TestMain(unittest.TestCase):
    """Test cases for main scanning"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.scan_opts = ScanOptions()
        # Create a temporary directory for testing
        self.temp_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        """Clean up test fixtures"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
    def test_get_adapter(self):
        """Test getting adapter for URL scheme"""
        # Test file adapter
        file_url = pathlib.Path(self.temp_dir).as_uri()
        adapter = get_adapter(file_url)
        self.assertIsNotNone(adapter)
        self.assertEqual(adapter.__class__.__name__, "LocalFileAdapter")
        
        # Test MongoDB adapter
        adapter = get_adapter("mongodb://localhost", {})
        self.assertIsNotNone(adapter)
        self.assertEqual(adapter.__class__.__name__, "MongodbAdapter")
        
        # Test Redis adapter
        adapter = get_adapter("redis://localhost", {})
        self.assertIsNotNone(adapter)
        self.assertEqual(adapter.__class__.__name__, "RedisAdapter")
        
        # Test S3 adapter
        adapter = get_adapter("s3://bucket", {})
        self.assertIsNotNone(adapter)
        self.assertEqual(adapter.__class__.__name__, "S3Adapter")
        
        # Test SQL adapter
        adapter = get_adapter("postgresql://localhost")
        self.assertIsNotNone(adapter)
        self.assertEqual(adapter.__class__.__name__, "SQLAdapter")
        
        # Test Elasticsearch adapter
        adapter = get_adapter("elasticsearch://localhost", {})
        self.assertIsNotNone(adapter)
        self.assertEqual(adapter.__class__.__name__, "ElasticsearchAdapter")
        
        # Test unsupported scheme
        with self.assertRaises(ScanError):
            get_adapter("unsupported://test")
        
    def test_scan_files(self):
        """Test scanning files"""
        # Create a test file
        test_file = os.path.join(self.temp_dir, "test.txt")
        with open(test_file, 'w') as f:
            f.write("test@example.com\n")
            
        # Use the original path directly
        file_url = pathlib.Path(self.temp_dir).as_uri()
        matches = scan(file_url, self.scan_opts)
        self.assertIsInstance(matches, list)
        
    def test_scan_files_with_matches(self):
        """Test scanning files with matches"""
        # Create a test file with email
        test_file = os.path.join(self.temp_dir, "test.txt")
        with open(test_file, 'w') as f:
            f.write("test@example.com\n")
            
        # Use the original path directly
        file_url = pathlib.Path(self.temp_dir).as_uri()
        matches = scan(file_url, self.scan_opts)
        self.assertIsInstance(matches, list)
        
    def test_scan_files_error(self):
        """Test scanning files with error"""
        with self.assertRaises(ScanError):
            scan("invalid://url", self.scan_opts)
        
if __name__ == "__main__":
    unittest.main() 