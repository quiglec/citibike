import boto3
import subprocess
import sys
import logging
from bs4 import BeautifulSoup
import requests

#s3 bucket URL
URL = "https://s3.amazonaws.com/tripdata/"

#Define target directory
trip_hdfs = '/user/clsadmin/data1'

def get_s3_ride_files(url: str) -> list:
    """
    Return list of ride files stored in CitiBike s3
    """
    r = requests.get(URL)
    soup = BeautifulSoup(r.content, 'html5lib')
    all_files = [key.text for key in soup.findAll('key')]
    ride_files = [x for x in all_files if x[:2]=='20' and x.count('-')<3]
    return ride_files

def get_hdfs_ride_files(hdfs_dir: str) -> list:
    """
    Return list of ride files already stored in HDFS
    """
    files = []
    output = subprocess.check_output(f'hdfs dfs -ls {hdfs_dir}', shell = True)
    files.append(str(output).split(hdfs_dir)[-1].strip('/').split(r'\\n')[0])
    return files

def find_file_diff(s3_files: list, hdfs_files: list) -> list:
    """
    Return s3 files not present in HDFS
    """
    diff = []
    for file in s3_files:
        if file not in hdfs_files:
            diff.append(file)
    return diff

def copy_ride_files_2_hdfs(url: str, hdfs_dir: str, ride_files: list):
    """
    Copy files from s3 to hdfs directory
    """
    for file in ride_files:
        subprocess.check_output(f'wget {url}{file} -P data', shell = True)
        subprocess.check_output(f'unzip data/{file} -d data', shell = True)
        subprocess.check_output(f'mv data/*.csv data/{file.split(".")[0].replace("-tripdata","") + ".csv"} ', shell = True)
        subprocess.check_output(f'hdfs dfs -put data/*.csv {hdfs_dir}', shell = True)
        subprocess.check_output(f'rm -rf data/*', shell = True)

    """
    for file in ride_files:
        logger.info(f'hdfs dfs -cp {url}{file} {hdfs_dir}')
        print(f'hdfs dfs -cp {url}{file} {hdfs_dir}')
        subprocess.check_output(f'hdfs dfs -cp {url}{file} {hdfs_dir}', shell = True)
        subprocess.check_output(f'hdfs dfs -get {hdfs_dir}/{file} data', shell = True)
        subprocess.check_output(f'hdfs dfs -rm {hdfs_dir}/{file}', shell = True)
        subprocess.check_output(f'unzip data/{file} -d data', shell = True)
        subprocess.check_output(f'rm -rf data/__MACOSX', shell = True)
        subprocess.check_output(f'rm -rf data/{file}', shell = True)
        subprocess.check_output(f'mv data/* data/{file.split(".")[0].replace("-tripdata","") + ".csv"}', shell = True)      
        subprocess.check_output(f'hdfs dfs -put data/* {hdfs_dir}', shell = True)
        subprocess.check_output(f'rm data/*', shell = True)
    """

def main():
    try:
        logger.info('Finding s3 files...')
        s3_files = get_s3_ride_files(URL)
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
        logger.info('Identifying missing files...')
        files_2_copy = find_file_diff(s3_files, hdfs_files)
    except Exception:
        logger.error('Unable to identify latest ride files')
        sys.exit(-1)

    try: 
        logger.info('Retrieving missing files...')
        copy_ride_files_2_hdfs(URL, trip_hdfs, files_2_copy)
    except Exception:
        logger.error('Unable to retrieve latest ride files')
        sys.exit(-1)

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    main()
