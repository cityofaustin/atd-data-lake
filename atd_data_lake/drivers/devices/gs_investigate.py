"""
gs_investigate contains routines for reading GRIDSMART counts file contents, used by gs_json_standard.py.

@author Kenneth Perrine, Nadia Florez
"""
from datetime import datetime as dt
import zipfile
import re
import os

from atd_data_lake.util import zip_helper, date_dirs

"Used for identifying MAC addresses in ZIP files"
MAC_PATTERN = re.compile("..\-..\-..\-..\-..\-..")

def investigate(zipFilePath, callback):
    """
    Cracks open ZIP file and ultimately yields on each GUID, CSV path.
    
    @return True if successful
    """
    
    # Identify the directory that matches the pattern:
    # (Or if you have the MAC address already, you don't have to search for it like this.)
    # TODO: This only finds the first camera directory; there are some detectors that have more than one.
    macDir = None
    try:
        zipFile = zip_helper.ZipHelper(zipFilePath)
    except (RuntimeError, zipfile.BadZipFile) as exc:
        print("Zip file was not extracted: Error message:")
        print(exc)
        return False
    findCount = 0
    for dirName in zipFile.getDirs(getFullPath=False):
        match = MAC_PATTERN.match(dirName)
        if match:
            macDir = match.group()
            findCount += 1
            print("MAC address directory #%d: %s" % (findCount, macDir))
            
            second = zipFile.getDirs(macDir)
            # TODO: For better checking, ensure that the located file matches the date format.
            if second:
                _investigateTypeA(zipFile, second[0], callback)
            else:
                second = zipFile.getFiles(macDir)
                if second and second[0].endswith(".zip"):
                    try:
                        _investigateTypeB(second[0], macDir, callback)
                    except (RuntimeError, zipfile.BadZipFile) as exc:
                        print("Zip file error was generated:")
                        print(exc)
                else:
                    print("Secondary file or directory not found!")            
            
    if not findCount:
        print("Could not find a MAC address directory!")
        return False

    # Clean up, although this should happen automatically when the program finishes.
    del zipFile
    return True

def _investigateTypeA(zipFile, secondDir, callback):
    print("Type A: Second directory: %s" % secondDir)
    date = date_dirs.DateDir(path=os.path.join(secondDir, ".."), prefix="", dateFormat="%Y-%m-%d", postfix="", findDirs=True).getDates()
    ##should only be a single date?
    date = dt.strftime(date[0], format="%Y-%m-%d")

    _investigateGUIDFiles(zipFile, secondDir, date, "A", callback)

def _investigateTypeB(secondFile, macDir, callback):
    print("Type B: Secondary file: %s" % secondFile)
    mySecondZipFile = zip_helper.ZipHelper(secondFile)
    date = secondFile[secondFile.rfind('/')+1:-4]

    _investigateGUIDFiles(mySecondZipFile, "", date, "B", callback)

    # Clean up before we clean up the parent archive.
    del mySecondZipFile

def _investigateGUIDFiles(myZipFile, secondDir, date, dir_type, callback):

    if dir_type == 'A':
        filenames = [os.path.join(secondDir, x) for x in os.listdir(secondDir)]
    elif dir_type == 'B':
        filenames = myZipFile.getFiles()

    print("Date: %s" % str(date))

    file_dict = {os.path.basename(file)[0:-4]:file for file in filenames}
    callback(file_dict)
