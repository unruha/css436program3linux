import boto3, botocore
import os, sys
s3 = boto3.resource('s3')

# restore from the remote directory to the local directory
def restoreDirectory(bucket_name, bucket_directory, local_directory):
    num_restored = 0
    bucket = s3.Bucket(bucket_name)

    # iterate through each object in the bucket
    for object in bucket.objects.filter(Prefix=bucket_directory):
        new_directory = os.path.join(local_directory, object.key)

        # if the new directory does not exist then create it
        if not os.path.exists(os.path.dirname(new_directory)):
            os.makedirs(os.path.dirname(new_directory))

        # move the file from the bucket directory into the local directory
        bucket.download_file(object.key, new_directory)
        num_restored = num_restored + 1
        print('Restoring File: ' + new_directory)
    print('Files Restored: ' + str(num_restored))

if len(sys.argv) != 3:
    print('error: invalid number of command line arguments. format should be: bucket-name::bucket-directory local-directory')
    print('note: if the directory name has space characters, enclose the directory name with double quotes')
    print('note: if the directory name is enclosed in quotes, DO NOT add a trailing backslash')
    sys.exit(0)

bucket_info = sys.argv[1]
local_directory = sys.argv[2]

bucket_info = bucket_info.split('::')
bucket_name = str.lower(bucket_info[0])
bucket_directory = bucket_info[1]

# remove trailing slashes in directory name
if local_directory.endswith('\\'):
    local_directory = str.lower(local_directory[:-1])

# check whether bucket is unique
try:
    s3.meta.client.head_bucket(Bucket=bucket_name)
except botocore.exceptions.ClientError as e:
    # If a client error is thrown, then check that it was a 404 error.
    # If it was a 404 error, then the bucket does not exist.
    error_code = int(e.response['Error']['Code'])
    if error_code == 403:
        print('ERROR: Cannot access this bucket')
        sys.exit(0)

restoreDirectory(bucket_name, bucket_directory, local_directory)