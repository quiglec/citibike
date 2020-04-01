import boto3
import subprocess

s3 = boto3.resource('s3')
my_bucket = s3.Bucket('tripdata')

def get_s3_ride_files(trip_bucket) -> list:
    """
    Return list of ride files stored in CitiBike s3
    """
    all_files = []
    for my_bucket_object in my_bucket.objects.all():
        all_files.append(my_bucket_object.key)
    ride_files = [x for x in all_files if x[:2]=="20" and x.count("-")<3]
    return ride_files

def get_hdfs_ride_files(hdfs_dir) -> list:
    """
    Return list of ride files already stored in HDFS
    """



def find_file_diff(s3_files, hdfs_files) -> list:
    """
    Return s3 files not present in HDFS
    """
    diff = []
    for file in s3_files:
        if file not in hdfs_files:
            diff.append(file)
    return diff

def copy_ride_files_s3_2_hdfs(trip_bucket, hdfs_dir, ride_files):
    """
    Copy files from s3 to hdfs directory
    """
