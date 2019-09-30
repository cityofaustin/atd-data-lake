'''
zip_helper.py contains the ZipHelper class that represents an unpacked ZIP file.

@author: Kenneth Perrine
'''

import os
import zipfile
import tempfile
import shutil

"""
ZipHelper represents a ZIP file. It unpacks the ZIP file upon creation, and then cleans up upon destruction.
"""
class ZipHelper:
    """
    __init__() is the constructor and attempts to unpack the given .ZIP file.
    """
    def __init__(self, zipFile):
        # Create the temporary directory:
        self.tempDir = tempfile.mkdtemp()
        
        # Extract the ZIP file contents:
        zipRef = zipfile.ZipFile(zipFile, 'r')
        zipRef.extractall(self.tempDir)

    """
    _getFiles() is used internally.
    """
    def _getFiles(self, path, getFullPath, findDirs):
        if not self.tempDir:
            return None
        if not path.startswith(self.tempDir):
            path = os.path.join(self.tempDir, path)
        
        ret = []
        for filename in os.listdir(path):
            fullFile = os.path.join(path, filename)
            if findDirs and os.path.isdir(fullFile) or not findDirs and os.path.isfile(fullFile):
                ret.append(fullFile if getFullPath else filename)
        return ret

    """
    getFiles() returns a list of files contained within the ZIP file at the given subdirectory, or the root"
    directory if no path supplied. Supply an optional directory path to list files within that directory.
    """
    def getFiles(self, path="", getFullPath=True):
        return self._getFiles(path, getFullPath, False)

    """
    getDirs() returns a list of directories contained within the ZIP file at the given subdirectory, or the root"
    directory if no path supplied. Supply an optional directory path to list files within that directory.
    """
    def getDirs(self, path="", getFullPath=True):
        return self._getFiles(path, getFullPath, True)
            
    """
    cleanup() removes the unpacked ZIP file contents.
    """
    def cleanup(self):
        shutil.rmtree(self.tempDir)
        self.tempDir = None

    """
    isOpen() returns True if the ZIP file is currently unpacked and available.
    """
    def isOpen(self):
        return True if self.tempDir else False
    
    """
    getUnpackPath() returns the path to the base of the unpacked ZIP file, or None if the ZIP file contents aren't available.
    """
    def getUnpackPath(self):
        return self.tempDir
    
    """
    __del__() calls cleanup() to automatically remove the unpacked ZIP file.
    """
    def __del__(self):
        self.cleanup()
