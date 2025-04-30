import os
import sys
import urllib.request
import zipfile
import shutil

def download_winutils():
    # Create hadoop directory if it doesn't exist
    hadoop_dir = os.path.join(os.environ['USERPROFILE'], 'hadoop')
    bin_dir = os.path.join(hadoop_dir, 'bin')
    
    if not os.path.exists(hadoop_dir):
        os.makedirs(hadoop_dir)
    if not os.path.exists(bin_dir):
        os.makedirs(bin_dir)
    
    # Download winutils.exe
    winutils_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.1/bin/winutils.exe"
    winutils_path = os.path.join(bin_dir, 'winutils.exe')
    
    if not os.path.exists(winutils_path):
        print("Downloading winutils.exe...")
        urllib.request.urlretrieve(winutils_url, winutils_path)
    
    # Set HADOOP_HOME environment variable
    os.environ['HADOOP_HOME'] = hadoop_dir
    
    # Add to system PATH if not already present
    if hadoop_dir not in os.environ['PATH']:
        os.environ['PATH'] = os.environ['PATH'] + ';' + bin_dir
    
    print(f"Hadoop environment set up successfully!")
    print(f"HADOOP_HOME: {os.environ['HADOOP_HOME']}")
    
if __name__ == "__main__":
    download_winutils()