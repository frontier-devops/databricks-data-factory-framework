from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.input_source.parquet_input_source import ParquetInputSource

from databricks.sdk.runtime import dbutils
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from pytz import timezone

class TestArchiveFiles(unittest.TestCase):    
    @patch('data_factory_framework.dlk.cdt.common.utils.Utils.get_logger')  # Mocking the ftrlog function
    @patch('databricks.sdk.runtime.dbutils.fs.mv')
    @patch('databricks.sdk.runtime.dbutils.fs.ls')
    @patch('data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.input_source.parquet_input_source.ParquetInputSource.archive_file_timezone')
    def test_archive_files(self, mock_get_logger, mock_archive_file_timezone, mock_ls, mock_mv):
        # Setup Mocks
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger  # Return the mock ftrlog
        mock_table_config = MagicMock()
        mock_table_config.source_loc = 's3://test-bucket/test-folder'
        
        # Mock datetime to return a fixed time
        mock_archive_file_timezone.now.return_value = datetime(2024, 9, 26, 12, 0, 0, tzinfo=timezone('US/Central'))
        
        # Mock files in source location
        mock_ls.return_value = [
            MagicMock(path='s3://test-bucket/test-folder/file1.parquet', name='file1.parquet'),
            MagicMock(path='s3://test-bucket/test-folder/file2.parquet', name='file2.parquet'),
            MagicMock(path='s3://test-bucket/test-folder/_checkpoint', name='_checkpoint')
        ]
        
        # Mock mv operation (success)
        mock_mv.side_effect = [True, True]  # Simulate successful move for two parquet files
        
        # Create instance of the class
        parquet_input_source_instance = ParquetInputSource()  # Replace with your actual class
        parquet_input_source_instance._logger = mock_logger
        parquet_input_source_instance._table_config = mock_table_config
        
        # Call the method
        result = parquet_input_source_instance._archive_files()

        # Assertions
        # mock_logger.info.assert_any_call("Archiving processed files to 's3://test-bucket/test-folder/09262024120000/'...")
        # mock_logger.info.assert_any_call("Archiving processed files completed successfully for 's3://test-bucket/test-folder'")
        
        mock_mv.assert_any_call('s3://test-bucket/test-folder/file1.parquet', 's3://test-bucket/test-folder/09262024120000/file1.parquet', True)
        mock_mv.assert_any_call('s3://test-bucket/test-folder/file2.parquet', 's3://test-bucket/test-folder/09262024120000/file2.parquet', True)

        self.assertTrue(result)

# unittest.main()

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
