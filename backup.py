# Adrian Unruh

import boto3, botocore
import sys
import os

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
my_session = boto3.session.Session()

# if file in local directory was modified more recently than the same file in s3 -> True
# if file in local directory was not modified more recently than the same file in s3 -> false
def is_modified(bucket_name, local_path, s3_path):
    s3_update_time = s3.Object(bucket_name, s3_path).last_modified.timestamp()
    local_update_time = os.path.getmtime(local_path)
    if local_update_time > s3_update_time:
        return True
    return False

# returns whether or not the file exists
def isfile_s3(bucket, key: str):
    objs = list(bucket.objects.filter(Prefix=key))
    return len(objs) == 1 and objs[0].key == key

# backs up the local directory to the cloud in a bucket name specified by the bucket input
def backupDirectory(local_directory, bucket_name, bucket_directory):
    # keep track of the number of files created and modified
    created_count = 0
    modified_count = 0
    bucket = s3.Bucket(bucket_name)
    # try to create the bucket
    try:
        s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={ 'LocationConstraint': my_session.region_name })
        print("Creating new bucket: ", bucket)
    # if it cannot be created then we are using an existing bucket that we own
    except:
        print("Using existing bucket: ", bucket_name)

    local_directory = local_directory.replace('\\', '/')

    # retreive the root file name of the directory to be backed up
    split_directory = local_directory.split('/')
    root_name = split_directory[len(split_directory) - 1]

    for root, dirs, files in os.walk(local_directory):
        for file in files:
            file_path = os.path.join(root, file)
            s3_path = file_path
            s3_path = bucket_directory + s3_path[s3_path.find(root_name):]
            s3_path = s3_path.replace('\\', '/')

            # if the file already exists in s3....
            if isfile_s3(bucket, s3_path):
                if is_modified(bucket_name, file_path, s3_path):
                    print('Modifying file: ' + s3_path)
                    s3.Bucket(bucket_name).upload_file(file_path, s3_path)
                    modified_count = modified_count + 1
            else:
                print('Creating File: ' + s3_path)
                s3.Bucket(bucket_name).upload_file(file_path, s3_path)
                created_count = created_count + 1

    print('\nFiles Created: ' + str(created_count))
    print('Files Modified: ' + str(modified_count))
            
if len(sys.argv) != 3:
    print('error: invalid number of command line arguments. format should be: backup directory-name bucket-name::directory-name')
    print('note: if the directory name has space characters, enclose the directory name with double quotes')
    print('note: if the directory name is enclosed in quotes, DO NOT add a trailing backslash')
    sys.exit(0)

# local directory to be backed up
local_directory = str.lower(sys.argv[1])
# directory and bucket to back up to
backup_directory = sys.argv[2]

temp_split = backup_directory.split('::')

# remote bucket name
bucket = ''
# bucket directory name
bucket_directory = ''

try:
    bucket = str.lower(temp_split[0])
    bucket_directory = temp_split[1] + '/'
except:
    # handle invalid input format
    print('error: invalid input format. format should be: backup directory-name bucket-name::directory-name')
    sys.exit(0)

# extra slash forward slashes in local directory name
if local_directory.endswith('/') or local_directory.endswith('\\'):
    local_directory = local_directory[:-1]

# check if the local directory is valid
if not os.path.isdir(local_directory):
    print('error: local directory does not exist. please double check the directory path')
    sys.exit(0)

# check whether bucket is unique
try:
    s3.meta.client.head_bucket(Bucket=bucket)
except botocore.exceptions.ClientError as e:
    # If a client error is thrown, then check that it was a 404 error.
    # If it was a 404 error, then the bucket does not exist.
    error_code = int(e.response['Error']['Code'])
    if error_code == 403:
        print('ERROR: Cannot access this bucket')
        sys.exit(0)

# replace all forward slashes with backslashes to be compatible with unix system
# local_directory = local_directory.replace('/', '\\')

backupDirectory(local_directory, bucket, bucket_directory)
    