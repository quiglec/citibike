import boto3
import subprocess
import logging

#Initialize s3 bucket
bucket_name = 'tripdata'
s3 = boto3.resource('s3')
trip_bucket = s3.Bucket(bucket_name)

#Define target directory
trip_hdfs = '/user/clsadmin/data'

def get_s3_ride_files(trip_bucket) -> list:
    """
    Return list of ride files stored in CitiBike s3
    """
    all_files = []
    for file in trip_bucket.objects.all():
        all_files.append(file.key)
    ride_files = [x for x in all_files if x[:2]=='20' and x.count('-')<3]
    return ride_files

def get_hdfs_ride_files(hdfs_dir) -> list:
    """
    Return list of ride files already stored in HDFS
    """
    output = subprocess.check_output(f'hdfs dfs -ls {hdfs_dir}', shell=True)
    files = str(output).split(hdfs_dir)[1].strip('/').split(r'\\n')[0]
    return files

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
    for file in ride_files:
        subprocess.check_output(f'hdfs dfs -cp https://s3.amazonaws.com/{trip_bucket}/{file} {hdfs_dir}')

def main():
    try:
        logger.info('Finding s3 files...')
        s3_files = get_s3_ride_files(trip_bucket)
    except Exception:
        logger.error('Could not read s3 files')
        sys.exit(-1)

    try:
        logger.info('Finding files already in HDFS...')
        hdfs_files = get_hdfs_ride_files(trip_hdfs)
    except Exception:
        logger.error('Could not read HDFS files')
        sys.exit(-1)

    try: 
        logger.info('Retrieving missing files...')
        files_2_copy = find_file_diff(s3_files, hdfs_files)
        copy_ride_files_s3_2_hdfs(bucket_name, trip_hdfs, hdfs_files)
    except Exception:
        logger.error('Unable to retrieve latest ride files')
        sys.exit(-1)

if __name__ == "__main__":
    main()
