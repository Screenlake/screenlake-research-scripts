
# S3 Batch Processing, Face Redaction, CSV Consolidation Script

This script provides a comprehensive solution for downloading, processing, and analyzing data from AWS S3. It handles batch downloads, unzips files, processes images for face redaction, and aggregates CSV data. Below is a breakdown of its main functionalities.

## Features

- **AWS S3 Connection**: Establishes a connection to AWS S3, using existing credentials or prompting the user for new ones.
- **Date Range Query**: Allows users to specify a date range to filter objects for download.
- **Batch Processing**: Downloads objects in batches, utilizing multiple processors for efficiency.
- **Image Processing**: Detects faces in images and applies either blurring or redaction.
- **CSV Aggregation**: Aggregates data from CSV files across different folders, combining them based on specified prefixes.

## Dependencies

- `boto3`
- `cv2` (OpenCV)
- `pandas`
- `pytz`
- `tqdm`
- `multiprocessing`
- `json`
- `pathlib`
- `uuid`
- `zipfile`
- `datetime`
- `logging`

## Usage

1. **Connect to AWS S3**: Modify the `connect_to_s3` function to suit your AWS setup.
2. **Set Date Range**: Use `get_date_range_from_user` to specify the period for the data you're interested in.
3. **Configure Batch Processing**: `set_batch_and_processors` lets you define the size of each download batch and the number of processors.
4. **Download and Process Data**: The script will download data in batches, unzip files, redact faces in images, and aggregate CSVs.
5. **Aggregate and Analyze Data**: Finally, combine CSV files for further analysis.

## Main Functions

### Connecting to AWS S3

```python
def connect_to_s3():
```

### Query by Date Range

```python
def query_by_date_range(s3, bucket_name, path, start_date, end_date):
```

### Set Batch and Processor Parameters

```python
def set_batch_and_processors():
```

### Download Zip Files in Batches

```python
def download_zip_files_in_batches(s3, bucket_name, path, start_date, end_date, zip_batch_size, num_processors, query_id):
```

### Process Child Folders and Unzip Asynchronously

```python
def process_child_folder_and_unzip_async(path):
```

For detailed implementation of each function, refer to the script itself.

## Running the Script

Ensure all dependencies are installed and AWS credentials are configured correctly. Run the script via the command line:

```bash
python redact_consolidate.py
```

Replace `script_name.py` with the name of your script file.
