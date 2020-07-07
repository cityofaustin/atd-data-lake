"""
storage_s3.py: Storage functions facilitated by Amazon AWS S3

Kenneth Perrine
Center for Transportation Research, The University of Texas at Austin
"""
import os
import boto3
import arrow

_AWS_SESSTION = None

def configAWS_S3(awsKey, awsSecretKey):
    global _AWS_SESSION
    
    _AWS_SESSION = boto3.Session(aws_access_key_id=awsKey, aws_secret_access_key=awsSecretKey)

class StorageS3:
    """
    Implements storage access functions using AWS S3.
    """    
    def __init__(self, repository):
        """
        Initializes the object to access the given repository (e.g. "bucket").
        
        @param repository: The name of the "bucket" or repository that will be accessed
        """
        self.S3 = _AWS_SESSION.resource('s3')
        self.repository = repository
        
    def makePath(self, base, ext, dataSource, collectionDate, filename=None):
        """
        Builds a storage path for the S3 bucket using the given base, ext, and collectionDate.
        
        @param filename: If this is supplied, then the path will include the filename.
        """
        if isinstance(collectionDate, str):
            collectionDate = arrow.get(collectionDate)
        path = "{year}/{month:0>2}/{day:0>2}/{dataSource}".format(year=collectionDate.year, \
                month=collectionDate.month, day=collectionDate.day, dataSource=dataSource)
        if filename:
            path += "/" + filename
        return path
    
    def extractFilename(self, path):
        """
        Extracts the filename from the given S3 path.
        """
        return path.split("/")[-1]
    
    def retrieveFilePath(self, path, destPath=".", deriveFilename=False):
        """
        retrieveFilePath(path) retrieves a resource at the given storage platform-specific path (presumably retrieved from the
        catalog) and returns a full path to the written file.
        
        @param path: The S3 path in the bucket that's labeled as "repository" in this object.
        @param destPath: A path to write the file to; otherwise, the temp directory will be used. May include a filename if destFilename is None.
        @param deriveFilename: If true, obtains the filename from the given S3 path.
        """
        if deriveFilename:
            destPath = os.path.join(destPath, self.extractFilename(path))
        self.S3.Bucket(self.repository).download_file(path, destPath)
        return destPath
        
    def retrieveBufferPath(self, path):
        """
        retrieveBufferPath retrieves a resource at the given storage platform-specific path and provides it as a buffer.
        """
        obj = self.S3.Object(self.repository, path)
        return obj.get()['Body'].read()
        
    def writeFile(self, sourceFile, path):
        """
        writeFile writes sourceFile to the target fully specified S3 path.
        """
        with open(sourceFile, 'rb') as fileObj:
            self.writeBuffer(fileObj, path)
        
    def writeBuffer(self, sourceBuffer, path):
        """
        writeBuffer writes the contents of the buffer into the target fully specified S3 path.
        """
        obj = self.S3.Object(self.repository, path)
        obj.put(Body=sourceBuffer)        
