'''
zip_helper.py contains the ZipHelper class that represents an unpacked ZIP file.

@author: Kenneth Perrine
'''

import os
import zipfile
import tempfile
import shutil

class ZipHelper:
    """
    ZipHelper represents a ZIP file. It unpacks the ZIP file upon creation, and then cleans up upon destruction.
    """
    def __init__(self, zipFile):
        """
        __init__() is the constructor and attempts to unpack the given .ZIP file.
        """
        # Create the temporary directory:
        self.tempDir = tempfile.mkdtemp()
        
        # Extract the ZIP file contents:
        zipRef = zipfile.ZipFile(zipFile, 'r')
        zipRef.extractall(self.tempDir)

    def _getFiles(self, path, getFullPath, findDirs):
        """
        _getFiles() is used internally.
        """
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

    def getFiles(self, path="", getFullPath=True):
        """
        getFiles() returns a list of files contained within the ZIP file at the given subdirectory, or the root"
        directory if no path supplied. Supply an optional directory path to list files within that directory.
        """
        return self._getFiles(path, getFullPath, False)

    def getDirs(self, path="", getFullPath=True):
        """
        getDirs() returns a list of directories contained within the ZIP file at the given subdirectory, or the root"
        directory if no path supplied. Supply an optional directory path to list files within that directory.
        """
        return self._getFiles(path, getFullPath, True)
            
    def cleanup(self):
        """
        cleanup() removes the unpacked ZIP file contents.
        """
        shutil.rmtree(self.tempDir)
        self.tempDir = None

    def isOpen(self):
        """
        isOpen() returns True if the ZIP file is currently unpacked and available.
        """
        return True if self.tempDir else False
    
    def getUnpackPath(self):
        """
        getUnpackPath() returns the path to the base of the unpacked ZIP file, or None if the ZIP file contents aren't available.
        """
        return self.tempDir
    
    def __del__(self):
        """
        __del__() calls cleanup() to automatically remove the unpacked ZIP file.
        """
        self.cleanup()
