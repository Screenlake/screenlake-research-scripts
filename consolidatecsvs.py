import multiprocessing
import json
import pathlib
import zipfile
import pandas as pd
import pytz
import boto3
from tqdm import tqdm
import logging

import os
import cv2
from PIL import Image
import numpy as np


s3 = boto3.client('s3')

# Load the pre-trained face detection model
face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')

# Step 1: Connect to AWS S3
def connect_to_s3():
    # Check if AWS credentials are already configured
    if os.path.exists(os.path.expanduser("~/.aws/credentials")) and os.path.exists(os.path.expanduser("~/.aws/config")):
        use_existing = input("AWS credentials are already configured. Do you want to use existing credentials? (y/n): ")
        if use_existing.lower() == 'y' or not use_existing:
            logging.info("Using existing AWS credentials.")
            s3 = boto3.client('s3')
        else:
            # Prompt the user for AWS credentials
            aws_access_key = input("Enter AWS Access Key: ")
            aws_secret_key = input("Enter AWS Secret Key: ")
            s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    else:
        # Prompt the user for AWS credentials
        aws_access_key = input("Enter AWS Access Key: ")
        aws_secret_key = input("Enter AWS Secret Key: ")
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

    return s3


import boto3
from datetime import datetime, timedelta


def get_date_range_from_user():
    start_date_str = input("Enter the start date (YYYY-MM-DD, or press Enter for 1 year ago): ")
    end_date_str = input("Enter the end date (YYYY-MM-DD, or press Enter for now): ")

    # Set default values for start and end dates
    if not start_date_str:
        start_date = datetime.now() - timedelta(days=365)
    else:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

    if not end_date_str:
        end_date = datetime.now()
    else:
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    if start_date > end_date:
        raise ValueError("Start date cannot be greater than end date.")

    return start_date, end_date


def list_folders_in_path(s3, bucket_name, path):
    # List folders under the specified path
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=path, Delimiter='/')
    common_prefixes = response.get('CommonPrefixes', [])
    folders = [prefix.get('Prefix') for prefix in common_prefixes]
    return folders


def get_bucket_and_path():
    default_bucket_name = "screenlake-zip-prod"
    s3 = boto3.client('s3')

    # Prompt for the S3 bucket (default: screenlake-zip-prod)
    bucket_name = input(f"Enter the S3 bucket name (default: {default_bucket_name}): ") or default_bucket_name

    # Start with the base path and list folders
    base_path = "academia/tenant/"
    selected_path = base_path
    selected_count = 0
    header = 'tenant'
    prevHeader = ''
    seen_version = False
    while True:
        folders = list_folders_in_path(s3, bucket_name, selected_path)
        if len(folders) >= 1:
            if folders[len(folders) - 1].endswith("panel/"):
                header = 'panel'
                selected_path = folders[len(folders) - 1]
                continue

            if "V_" in folders[len(folders) - 1] and not seen_version:
                seen_version = True
                header = 'version'

            # Ensure the selected path ends with 'panelist'
            # if folders[len(folders) - 1].endswith("panelist/"):
            #     selected_path = selected_path + "panelist"

            if prevHeader == 'panelist':
                header = 'Here is a list of panelist who will have CSVs created'

        prevHeader = header
        if not folders:
            raise ValueError(f"No folders found at '{selected_path}'")

        if folders[len(folders) - 1].endswith("panelist/"):
            choice = '1'
            selected_count = selected_count + 1
            header = 'Here is a list of panelist who will have CSVs created'

            return bucket_name, selected_path
            # # Display folder options for user selection
            # logging.info(f'Select a {header}:')
            # for i, folder in enumerate(folders, start=1):
            #     split_folders = folder.split('/')
            #     folder_name = split_folders[len(split_folders) - 2]
            #     logging.info(f"{i}. {folder_name}")
        else:
            # Display folder options for user selection
            logging.info(f'Select a {header}:')
            for i, folder in enumerate(folders, start=1):
                split_folders = folder.split('/')
                folder_name = split_folders[len(split_folders) - 2]
                print(f"{i}. {folder_name}")

            choice = input("Enter the number corresponding to your choice (q to quit): ")
            selected_count = selected_count + 1

        if choice.lower() == 'q':
            break

        try:
            if not choice:
                choice = 1
            choice = int(choice)
            if 1 <= choice <= len(folders):
                selected_folder = folders[choice - 1]
                selected_path = selected_folder
            else:
                logging.info("Invalid choice. Please enter a valid number.")
        except ValueError:
            logging.info("Invalid input. Please enter a number or 'q' to quit.")

    return bucket_name, selected_path


# Step 3: Query by date range
def query_by_date_range(s3, bucket_name, path, start_date, end_date):
    objects = []
    next_token = None

    while True:
        params = {
            'Bucket': bucket_name,
            'Prefix': path,
            'ContinuationToken': next_token
        }

        response = s3.list_objects_v2(**params)
        objects.extend(response.get('Contents', []))
        next_token = response.get('NextContinuationToken')

        if not next_token:
            break

    filtered_objects = []

    for obj in objects:
        last_modified = obj['LastModified']
        if start_date <= last_modified <= end_date:
            filtered_objects.append(obj)

    return filtered_objects


# Helper function: Calculate total size of files in the date range
def calculate_total_size(s3, bucket_name, path, start_date, end_date):
    # Add logic to calculate the total size of files in the date range
    pass


# Step 4: Set batch and processor parameters
def set_batch_and_processors():
    zip_batch_size = int(input("Enter the batch size for zip file downloads (default: 25): ") or 25)
    num_processors = int(input("Enter the number of processors (default: 4): ") or 4)

    # query_id = f"query_{uuid.uuid4()}"
    query_id = "query_test"
    query_config = {
        "queryId": query_id,
        "batchSize": zip_batch_size,
        "numFilesToDownload": 0,
        "numFilesDownloaded": 0,
        "lastBatchBeginId": ""
    }

    # Check if the folder with query_id exists, and create it if it doesn't
    query_folder = os.path.join(query_id)
    if not os.path.exists(query_folder):
        os.makedirs(query_folder)

    zipped_folder = os.path.join(query_id, "zipped")
    if not os.path.exists(zipped_folder):
        os.makedirs(zipped_folder)

    unzipped_folder = os.path.join(query_id, "../unzipped")
    if not os.path.exists(unzipped_folder):
        os.makedirs(unzipped_folder)

    with open(f"{query_id}/query_config.json", "w") as config_file:
        json.dump(query_config, config_file)

    return zip_batch_size, num_processors, query_id


# Step 5: Download zip files in batches
def download_zip_files_in_batches(s3, bucket_name, path, start_date, end_date, zip_batch_size, num_processors,
                                  query_id):
    objects_to_download = query_s3_objects_in_date_range(s3, bucket_name, path, start_date, end_date)
    num_objects = len(objects_to_download)

    # Divide the objects into batches
    object_batches = [objects_to_download[i:i + zip_batch_size] for i in range(0, num_objects, zip_batch_size)]

    processes = []
    downloaded_count = 0
    with tqdm(total=len(object_batches), desc="Batches") as pbar_batch:
        for batch in object_batches:
            if len(processes) == num_processors or len(processes) == len(object_batches):
                for process in processes:
                    pbar_batch.update(1)
                    process.join()

                processes = []
            else:
                process = multiprocessing.Process(target=download_batch,
                                                  args=(batch, query_id, bucket_name))
                process.start()
                processes.append(process)

            if len(processes) == len(object_batches):
                for process in processes:
                    process.join()


def download_batch(batch, query_id, bucket_name):
    with tqdm(total=len(batch), desc="Downloading files") as pbar:
        for obj in batch:
            # Download the object using S3 client
            file_split = obj['Key'].split('/')
            folder = file_split[len(file_split) - 2]
            query_folder = os.path.join(f'{query_id}/zipped/{folder}')
            file_name = file_split[len(file_split) - 1]

            if not os.path.exists(query_folder):
                os.makedirs(query_folder)

            key = f"{query_id}/zipped/{folder}/{file_name}"
            if not os.path.exists(key):
                s3.download_file(bucket_name, obj['Key'], key)
                pbar.update(1)


# Step 6: Unzip and save CSVs
# def unzip_and_save_csvs(zip_file_path, output_folder):
#     with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
#         for file_name in zip_ref.namelist():
#             if file_name.endswith('.csv'):
#                 zip_ref.extract(file_name, output_folder)


def query_s3_objects_in_date_range(s3, bucket_name, path, start_date, end_date):
    objects = []

    next_token = None

    count = 0  # Initialize a count variable to track the number of files fetched

    while True:
        params = {
            'Bucket': bucket_name,
            'Prefix': path
        }

        if next_token:
            params['ContinuationToken'] = next_token  # Include ContinuationToken if it exists

        response = s3.list_objects_v2(**params)
        objects.extend(response.get('Contents', []))

        if 'NextContinuationToken' in response:
            next_token = response['NextContinuationToken']
        else:
            break  # Exit the loop when there are no more objects to fetch

    filtered_objects = []

    for obj in objects:
        start_date = start_date.replace(tzinfo=pytz.UTC)
        end_date = end_date.replace(tzinfo=pytz.UTC)
        last_modified = obj['LastModified']
        last_modified = last_modified.replace(tzinfo=pytz.UTC)  # Make sure LastModified is timezone-aware
        if start_date <= last_modified <= end_date:
            filtered_objects.append(obj)
            count += 1  # Increment the count for each file fetched
            logging.info(f"Files fetched: {count}", end='\r')  # Print the count with carriage return to overwrite the line

    # Print a newline to separate the progress count
    logging.info("")

    return filtered_objects


def unzip_file(zip_file, destination_folder, image_folder):
    try:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            count_of_existing_files = 0
            count_of_non_existing_files = 0
            for file_info in zip_ref.infolist():
                # Extract files ending in .csv
                if file_info.filename.endswith('.csv'):
                    zip_ref.extract(file_info, destination_folder)
                    # logging.info(f"Unzipped {file_info.filename} to {destination_folder}")
                # Check if the file is a JPG image
                elif file_info.filename.lower().endswith('.jpg') or file_info.filename.lower().endswith('.jpeg'):
                    # Construct the full path of the extracted file
                    extracted_file_path = os.path.join(destination_folder, file_info.filename)
                    extracted_image_folder_file_path = os.path.join(image_folder, file_info.filename)

                    # Check if the file already exists in the destination folder
                    if not os.path.exists(extracted_file_path):
                        # Extract the image
                        zip_ref.extract(file_info, destination_folder)
                        logging.info(f"Unzipped {file_info.filename} to {destination_folder}")

                        # Full path of the extracted image
                        image_path = extracted_file_path

                        # Detect and redact faces in the image
                        detect_and_redact_faces(image_path, extracted_image_folder_file_path)  # Overwrite the image
                        count_of_non_existing_files = + 1
                    else:
                        count_of_existing_files =+ 1

            logging.info(f"Count of already exists files {count_of_existing_files}.")
            logging.info(f"Count of processed files {count_of_non_existing_files}.")

            try:
                os.remove(zip_ref.filename)
                # logging.info(f"File '{zip_ref.filename}' has been deleted.")
            except OSError as e:
                logging.info(f"Error deleting file: {str(e)}")

    except Exception as e:
        logging.info(f"Error unzipping {zip_file}: {str(e)}")

import multiprocessing
import json
import pathlib
import zipfile
import pandas as pd
import pytz
import boto3
from tqdm import tqdm
import logging

import os
import cv2
from PIL import Image
import numpy as np


s3 = boto3.client('s3')

# Load the pre-trained face detection model
face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')

# Step 1: Connect to AWS S3
def connect_to_s3():
    # Check if AWS credentials are already configured
    if os.path.exists(os.path.expanduser("~/.aws/credentials")) and os.path.exists(os.path.expanduser("~/.aws/config")):
        use_existing = input("AWS credentials are already configured. Do you want to use existing credentials? (y/n): ")
        if use_existing.lower() == 'y' or not use_existing:
            logging.info("Using existing AWS credentials.")
            s3 = boto3.client('s3')
        else:
            # Prompt the user for AWS credentials
            aws_access_key = input("Enter AWS Access Key: ")
            aws_secret_key = input("Enter AWS Secret Key: ")
            s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    else:
        # Prompt the user for AWS credentials
        aws_access_key = input("Enter AWS Access Key: ")
        aws_secret_key = input("Enter AWS Secret Key: ")
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

    return s3


import boto3
from datetime import datetime, timedelta


def get_date_range_from_user():
    start_date_str = input("Enter the start date (YYYY-MM-DD, or press Enter for 1 year ago): ")
    end_date_str = input("Enter the end date (YYYY-MM-DD, or press Enter for now): ")

    # Set default values for start and end dates
    if not start_date_str:
        start_date = datetime.now() - timedelta(days=365)
    else:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

    if not end_date_str:
        end_date = datetime.now()
    else:
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    if start_date > end_date:
        raise ValueError("Start date cannot be greater than end date.")

    return start_date, end_date


def list_folders_in_path(s3, bucket_name, path):
    # List folders under the specified path
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=path, Delimiter='/')
    common_prefixes = response.get('CommonPrefixes', [])
    folders = [prefix.get('Prefix') for prefix in common_prefixes]
    return folders


def get_bucket_and_path():
    default_bucket_name = "screenlake-zip-prod"
    s3 = boto3.client('s3')

    # Prompt for the S3 bucket (default: screenlake-zip-prod)
    bucket_name = input(f"Enter the S3 bucket name (default: {default_bucket_name}): ") or default_bucket_name

    # Start with the base path and list folders
    base_path = "academia/tenant/"
    selected_path = base_path
    selected_count = 0
    header = 'tenant'
    prevHeader = ''
    seen_version = False
    while True:
        folders = list_folders_in_path(s3, bucket_name, selected_path)
        if len(folders) >= 1:
            if folders[len(folders) - 1].endswith("panel/"):
                header = 'panel'
                selected_path = folders[len(folders) - 1]
                continue

            if "V_" in folders[len(folders) - 1] and not seen_version:
                seen_version = True
                header = 'version'

            # Ensure the selected path ends with 'panelist'
            # if folders[len(folders) - 1].endswith("panelist/"):
            #     selected_path = selected_path + "panelist"

            if prevHeader == 'panelist':
                header = 'Here is a list of panelist who will have CSVs created'

        prevHeader = header
        if not folders:
            raise ValueError(f"No folders found at '{selected_path}'")

        if folders[len(folders) - 1].endswith("panelist/"):
            choice = '1'
            selected_count = selected_count + 1
            header = 'Here is a list of panelist who will have CSVs created'

            return bucket_name, selected_path
            # # Display folder options for user selection
            # logging.info(f'Select a {header}:')
            # for i, folder in enumerate(folders, start=1):
            #     split_folders = folder.split('/')
            #     folder_name = split_folders[len(split_folders) - 2]
            #     logging.info(f"{i}. {folder_name}")
        else:
            # Display folder options for user selection
            logging.info(f'Select a {header}:')
            for i, folder in enumerate(folders, start=1):
                split_folders = folder.split('/')
                folder_name = split_folders[len(split_folders) - 2]
                print(f"{i}. {folder_name}")

            choice = input("Enter the number corresponding to your choice (q to quit): ")
            selected_count = selected_count + 1

        if choice.lower() == 'q':
            break

        try:
            if not choice:
                choice = 1
            choice = int(choice)
            if 1 <= choice <= len(folders):
                selected_folder = folders[choice - 1]
                selected_path = selected_folder
            else:
                logging.info("Invalid choice. Please enter a valid number.")
        except ValueError:
            logging.info("Invalid input. Please enter a number or 'q' to quit.")

    return bucket_name, selected_path


# Step 3: Query by date range
def query_by_date_range(s3, bucket_name, path, start_date, end_date):
    objects = []
    next_token = None

    while True:
        params = {
            'Bucket': bucket_name,
            'Prefix': path,
            'ContinuationToken': next_token
        }

        response = s3.list_objects_v2(**params)
        objects.extend(response.get('Contents', []))
        next_token = response.get('NextContinuationToken')

        if not next_token:
            break

    filtered_objects = []

    for obj in objects:
        last_modified = obj['LastModified']
        if start_date <= last_modified <= end_date:
            filtered_objects.append(obj)

    return filtered_objects


# Helper function: Calculate total size of files in the date range
def calculate_total_size(s3, bucket_name, path, start_date, end_date):
    # Add logic to calculate the total size of files in the date range
    pass


# Step 4: Set batch and processor parameters
def set_batch_and_processors():
    zip_batch_size = int(input("Enter the batch size for zip file downloads (default: 25): ") or 25)
    num_processors = int(input("Enter the number of processors (default: 4): ") or 4)

    # query_id = f"query_{uuid.uuid4()}"
    query_id = "query_test"
    query_config = {
        "queryId": query_id,
        "batchSize": zip_batch_size,
        "numFilesToDownload": 0,
        "numFilesDownloaded": 0,
        "lastBatchBeginId": ""
    }

    # Check if the folder with query_id exists, and create it if it doesn't
    query_folder = os.path.join(query_id)
    if not os.path.exists(query_folder):
        os.makedirs(query_folder)

    zipped_folder = os.path.join(query_id, "zipped")
    if not os.path.exists(zipped_folder):
        os.makedirs(zipped_folder)

    unzipped_folder = os.path.join(query_id, "../unzipped")
    if not os.path.exists(unzipped_folder):
        os.makedirs(unzipped_folder)

    with open(f"{query_id}/query_config.json", "w") as config_file:
        json.dump(query_config, config_file)

    return zip_batch_size, num_processors, query_id


# Step 5: Download zip files in batches
def download_zip_files_in_batches(s3, bucket_name, path, start_date, end_date, zip_batch_size, num_processors,
                                  query_id):
    objects_to_download = query_s3_objects_in_date_range(s3, bucket_name, path, start_date, end_date)
    num_objects = len(objects_to_download)

    # Divide the objects into batches
    object_batches = [objects_to_download[i:i + zip_batch_size] for i in range(0, num_objects, zip_batch_size)]

    processes = []
    downloaded_count = 0
    with tqdm(total=len(object_batches), desc="Batches") as pbar_batch:
        for batch in object_batches:
            if len(processes) == num_processors or len(processes) == len(object_batches):
                for process in processes:
                    pbar_batch.update(1)
                    process.join()

                processes = []
            else:
                process = multiprocessing.Process(target=download_batch,
                                                  args=(batch, query_id, bucket_name))
                process.start()
                processes.append(process)

            if len(processes) == len(object_batches):
                for process in processes:
                    process.join()


def download_batch(batch, query_id, bucket_name):
    with tqdm(total=len(batch), desc="Downloading files") as pbar:
        for obj in batch:
            # Download the object using S3 client
            file_split = obj['Key'].split('/')
            folder = file_split[len(file_split) - 2]
            query_folder = os.path.join(f'{query_id}/zipped/{folder}')
            file_name = file_split[len(file_split) - 1]

            if not os.path.exists(query_folder):
                os.makedirs(query_folder)

            key = f"{query_id}/zipped/{folder}/{file_name}"
            if not os.path.exists(key):
                s3.download_file(bucket_name, obj['Key'], key)
                pbar.update(1)


# Step 6: Unzip and save CSVs
# def unzip_and_save_csvs(zip_file_path, output_folder):
#     with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
#         for file_name in zip_ref.namelist():
#             if file_name.endswith('.csv'):
#                 zip_ref.extract(file_name, output_folder)


def query_s3_objects_in_date_range(s3, bucket_name, path, start_date, end_date):
    objects = []

    next_token = None

    count = 0  # Initialize a count variable to track the number of files fetched

    while True:
        params = {
            'Bucket': bucket_name,
            'Prefix': path
        }

        if next_token:
            params['ContinuationToken'] = next_token  # Include ContinuationToken if it exists

        response = s3.list_objects_v2(**params)
        objects.extend(response.get('Contents', []))

        if 'NextContinuationToken' in response:
            next_token = response['NextContinuationToken']
        else:
            break  # Exit the loop when there are no more objects to fetch

    filtered_objects = []

    for obj in objects:
        start_date = start_date.replace(tzinfo=pytz.UTC)
        end_date = end_date.replace(tzinfo=pytz.UTC)
        last_modified = obj['LastModified']
        last_modified = last_modified.replace(tzinfo=pytz.UTC)  # Make sure LastModified is timezone-aware
        if start_date <= last_modified <= end_date:
            filtered_objects.append(obj)
            count += 1  # Increment the count for each file fetched
            logging.info(f"Files fetched: {count}", end='\r')  # Print the count with carriage return to overwrite the line

    # Print a newline to separate the progress count
    logging.info("")

    return filtered_objects


def unzip_file(zip_file, destination_folder, image_folder):
    try:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            count_of_existing_files = 0
            count_of_non_existing_files = 0
            for file_info in zip_ref.infolist():
                # Extract files ending in .csv
                if file_info.filename.endswith('.csv'):
                    zip_ref.extract(file_info, destination_folder)
                    # logging.info(f"Unzipped {file_info.filename} to {destination_folder}")
                # Check if the file is a JPG image
                elif file_info.filename.lower().endswith('.jpg') or file_info.filename.lower().endswith('.jpeg'):
                    # Construct the full path of the extracted file
                    extracted_file_path = os.path.join(destination_folder, file_info.filename)
                    extracted_image_folder_file_path = os.path.join(image_folder, file_info.filename)

                    # Check if the file already exists in the destination folder
                    if not os.path.exists(extracted_file_path):
                        # Extract the image
                        zip_ref.extract(file_info, destination_folder)
                        logging.info(f"Unzipped {file_info.filename} to {destination_folder}")

                        # Full path of the extracted image
                        image_path = extracted_file_path

                        # Detect and redact faces in the image
                        detect_and_redact_faces(image_path, extracted_image_folder_file_path)  # Overwrite the image
                        count_of_non_existing_files = + 1
                    else:
                        count_of_existing_files =+ 1

            logging.info(f"Count of already exists files {count_of_existing_files}.")
            logging.info(f"Count of processed files {count_of_non_existing_files}.")

            try:
                os.remove(zip_ref.filename)
                # logging.info(f"File '{zip_ref.filename}' has been deleted.")
            except OSError as e:
                logging.info(f"Error deleting file: {str(e)}")

    except Exception as e:
        logging.info(f"Error unzipping {zip_file}: {str(e)}")


def process_child_folder(parent, children, query_id):
    child_folder_split = parent.split('/')
    panelist_folder = child_folder_split[len(parent.split('/')) - 1]

    unzipped_folder = "unzipped"

    os.makedirs(unzipped_folder, exist_ok=True)

    split_unzipped_name = parent.split('/')
    unzipped_name = split_unzipped_name[0]

    os.path.join(unzipped_name, f'{query_id}/panelists/unzipped')

    for child in children:
        # Unzip files in the child folder
        pathlib.Path(f'{query_id}/combined/panelists/{panelist_folder}/images').mkdir(parents=True, exist_ok=True)
        unzip_file(os.path.join(parent, child), f'{query_id}/unzipped/panelists/{panelist_folder}', f'{query_id}/combined/panelists/{panelist_folder}/images')


def process_child_folders(path):
    zipped_child_folders = []
    result_dict = {}
    # Get a list of child folders with their full paths
    for root, dirs, files in os.walk(path):
        for dir_name in dirs:
            child_folder = os.path.join(root, dir_name)
            zipped_child_folders.append(child_folder)

            child_files = [file for file in os.listdir(child_folder) if file.endswith('.zip')]
            result_dict[child_folder] = child_files

    # Create a pool of worker processes
    num_processes = multiprocessing.cpu_count()
    processes = []

    for key in result_dict:
        if len(processes) == num_processes:
            for process in processes:
                process.join()

            processes = []
        else:
            process = multiprocessing.Process(target=process_child_folder,
                                              args=(key, list(result_dict[key]), path))
            process.start()
            processes.append(process)

        if len(processes) == len(zipped_child_folders):
            for process in processes:
                process.join()


def process_child_folders_csvs(path):
    zipped_child_folders = []
    result_dict = {}
    # Get a list of child folders with their full paths
    for root, dirs, files in os.walk(path):
        for dir_name in dirs:
            child_folder = os.path.join(root, dir_name)
            zipped_child_folders.append(child_folder)

            child_files = [os.path.join(child_folder, file) for file in os.listdir(child_folder) if file.endswith('.csv')]
            base_name = os.path.basename(child_folder)
            if base_name != 'panelist' or base_name != 'panelists':
                result_dict[os.path.basename(child_folder)] = child_files
    return result_dict

def detect_and_redact_faces(input_image_path, output_image_path, redaction_type='redact'):
    # Load the input image
    image = cv2.imread(input_image_path)
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Detect faces in the image
    faces = face_cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30))

    # Process each detected face
    for (x, y, w, h) in faces:
        # Extract the face region
        face = image[y:y+h, x:x+w]

        if redaction_type == 'blur':
            # Blur the detected face
            face = cv2.GaussianBlur(face, (99, 99), 30)
        elif redaction_type == 'redact':
            # Redact the detected face by filling it with a solid color (black)
            face[:] = [0, 0, 0]

        # Replace the face in the original image with the redacted/blur face
        image[y:y+h, x:x+w] = face

    # Save the result
    cv2.imwrite(output_image_path, image)

def combine_csv_files(input_dict, query_id):
    output_folder = f"{query_id}/combined"

    for folder, csv_files in input_dict.items():
        prefix_dict = {}

        # Group CSV files by prefixƒ
        for csv_file in csv_files:
            filename = os.path.basename(csv_file)

            # Check if the prefix matches one of the specified prefixes
            valid_prefixes = ["screenshot_data", "app_accessibility_data", "app_segment_data", "session_data"]
            for prefix in valid_prefixes:
                if filename.startswith(prefix):
                    if prefix not in prefix_dict:
                        prefix_dict[prefix] = []
                        prefix_dict[prefix].append(csv_file)
                    else:
                        prefix_dict[prefix].append(csv_file)

        panelist_folder = os.path.join(os.path.join(output_folder, '../panelists'), folder + '/metadata')
        print(panelist_folder)
        pathlib.Path(panelist_folder).mkdir(parents=True, exist_ok=True)

        # Combine CSV files within each prefix group
        for prefix, files in prefix_dict.items():
            combined_csv = os.path.join(panelist_folder, f"{prefix}-consolidated.csv")
            header_written = False


            for file in files:
                df = pd.read_csv(file)
                if not header_written:
                    df.to_csv(combined_csv, index=False)
                    header_written = True
                else:
                    df.to_csv(combined_csv, mode='a', header=False, index=False)


# if __name__ == "__main__":
#     # Example usage:
#     folder_dict = {
#         "folder1": ["folder1/screenshot_data_1.csv", "folder1/screenshot_data_2.csv", "folder1/screenshot_data_3.csv"],
#         "folder2": ["folder2/accessibility_data_1.csv", "folder2/accessibility_data_2.csv"],
#         "folder3": ["folder3/app_segement_data_1.csv", "folder3/app_segement_data_2.csv"],
#         "folder4": ["folder4/session_data_1.csv"]
#     }
#
#     combine_csv_files(folder_dict)


# Step 7: Main function to orchestrate the workflow
def main():
    s3 = connect_to_s3()
    bucket_name, path = get_bucket_and_path()
    start_date, end_date = get_date_range_from_user()

    zip_batch_size, num_processors, query_id = set_batch_and_processors()
    download_zip_files_in_batches(s3, bucket_name, path, start_date, end_date, zip_batch_size, num_processors, query_id)

    test_id = 'query_5dfe6586-2f5c-462a-8d7b-67baf8805b6c'
    zipped = f'{query_id}/zipped'
    unzipped = f'{query_id}/unzipped'
    # query_id = 'query_fa9a713f-2e86-40e6-9d6d-74013b69b391'
    path = os.path.join(test_id)
    process_child_folders(query_id)



    result = process_child_folders_csvs(os.path.join(query_id, '../unzipped'))
    combine_csv_files(result, query_id)


if __name__ == "__main__":
    main()

def process_child_folder(parent, children, query_id):
    child_folder_split = parent.split('/')
    panelist_folder = child_folder_split[len(parent.split('/')) - 1]

    unzipped_folder = "unzipped"

    os.makedirs(unzipped_folder, exist_ok=True)

    split_unzipped_name = parent.split('/')
    unzipped_name = split_unzipped_name[0]

    os.path.join(unzipped_name, f'{query_id}/panelists/unzipped')

    for child in children:
        # Unzip files in the child folder
        pathlib.Path(f'{query_id}/combined/panelists/{panelist_folder}/images').mkdir(parents=True, exist_ok=True)
        unzip_file(os.path.join(parent, child), f'{query_id}/unzipped/panelists/{panelist_folder}', f'{query_id}/combined/panelists/{panelist_folder}/images')


def process_child_folders(path):
    zipped_child_folders = []
    result_dict = {}
    # Get a list of child folders with their full paths
    for root, dirs, files in os.walk(path):
        for dir_name in dirs:
            child_folder = os.path.join(root, dir_name)
            zipped_child_folders.append(child_folder)

            child_files = [file for file in os.listdir(child_folder) if file.endswith('.zip')]
            result_dict[child_folder] = child_files

    # Create a pool of worker processes
    num_processes = multiprocessing.cpu_count()
    processes = []

    for key in result_dict:
        if len(processes) == num_processes:
            for process in processes:
                process.join()

            processes = []
        else:
            process = multiprocessing.Process(target=process_child_folder,
                                              args=(key, list(result_dict[key]), path))
            process.start()
            processes.append(process)

        if len(processes) == len(zipped_child_folders):
            for process in processes:
                process.join()


def process_child_folders_csvs(path):
    zipped_child_folders = []
    result_dict = {}
    # Get a list of child folders with their full paths
    for root, dirs, files in os.walk(path):
        for dir_name in dirs:
            child_folder = os.path.join(root, dir_name)
            zipped_child_folders.append(child_folder)

            child_files = [os.path.join(child_folder, file) for file in os.listdir(child_folder) if file.endswith('.csv')]
            base_name = os.path.basename(child_folder)
            if base_name != 'panelist' or base_name != 'panelists':
                result_dict[os.path.basename(child_folder)] = child_files
    return result_dict

def detect_and_redact_faces(input_image_path, output_image_path, redaction_type='redact'):
    # Load the input image
    image = cv2.imread(input_image_path)
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Detect faces in the image
    faces = face_cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30))

    # Process each detected face
    for (x, y, w, h) in faces:
        # Extract the face region
        face = image[y:y+h, x:x+w]

        if redaction_type == 'blur':
            # Blur the detected face
            face = cv2.GaussianBlur(face, (99, 99), 30)
        elif redaction_type == 'redact':
            # Redact the detected face by filling it with a solid color (black)
            face[:] = [0, 0, 0]

        # Replace the face in the original image with the redacted/blur face
        image[y:y+h, x:x+w] = face

    # Save the result
    cv2.imwrite(output_image_path, image)

def combine_csv_files(input_dict, query_id):
    output_folder = f"{query_id}/combined"

    for folder, csv_files in input_dict.items():
        prefix_dict = {}

        # Group CSV files by prefixƒ
        for csv_file in csv_files:
            filename = os.path.basename(csv_file)

            # Check if the prefix matches one of the specified prefixes
            valid_prefixes = ["screenshot_data", "app_accessibility_data", "app_segment_data", "session_data"]
            for prefix in valid_prefixes:
                if filename.startswith(prefix):
                    if prefix not in prefix_dict:
                        prefix_dict[prefix] = []
                        prefix_dict[prefix].append(csv_file)
                    else:
                        prefix_dict[prefix].append(csv_file)

        panelist_folder = os.path.join(os.path.join(output_folder, '../panelists'), folder + '/metadata')
        print(panelist_folder)
        pathlib.Path(panelist_folder).mkdir(parents=True, exist_ok=True)

        # Combine CSV files within each prefix group
        for prefix, files in prefix_dict.items():
            combined_csv = os.path.join(panelist_folder, f"{prefix}-consolidated.csv")
            header_written = False


            for file in files:
                df = pd.read_csv(file)
                if not header_written:
                    df.to_csv(combined_csv, index=False)
                    header_written = True
                else:
                    df.to_csv(combined_csv, mode='a', header=False, index=False)


# if __name__ == "__main__":
#     # Example usage:
#     folder_dict = {
#         "folder1": ["folder1/screenshot_data_1.csv", "folder1/screenshot_data_2.csv", "folder1/screenshot_data_3.csv"],
#         "folder2": ["folder2/accessibility_data_1.csv", "folder2/accessibility_data_2.csv"],
#         "folder3": ["folder3/app_segement_data_1.csv", "folder3/app_segement_data_2.csv"],
#         "folder4": ["folder4/session_data_1.csv"]
#     }
#
#     combine_csv_files(folder_dict)


# Step 7: Main function to orchestrate the workflow
def main():
    s3 = connect_to_s3()
    bucket_name, path = get_bucket_and_path()
    start_date, end_date = get_date_range_from_user()

    zip_batch_size, num_processors, query_id = set_batch_and_processors()
    download_zip_files_in_batches(s3, bucket_name, path, start_date, end_date, zip_batch_size, num_processors, query_id)
    process_child_folders(query_id)



    result = process_child_folders_csvs(os.path.join(query_id, '../unzipped'))
    combine_csv_files(result, query_id)


if __name__ == "__main__":
    main()
