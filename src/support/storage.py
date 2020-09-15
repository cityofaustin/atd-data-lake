"""
storage.py: Facilitates common Data Lake storage functions, with tracking with catalog

Kenneth Perrine
Center for Transportation Research, The University of Texas at Austin
"""
import tempfile
import json

import os
import arrow

class Storage:
    """
    Facilitates the storage or retrieval of files within a cloud service or local volume
    """
    
    def __init__(self, storageConn, repository, dataSource, catalogResource=None, tempDir=None, simulationMode=False, writeFilePath=None):
        """
        Initializes storage connection using the application object
        
        @param storageConn: Implements the actual storage operations (a "driver"): implements StorageImpl.
        @param repository: The repository or bucket name that will be accessed
        @param dataSource: The dataSource abbreviation that is to be accessed
        @param catalogResource: A Catalog object
        @param tempDir: An already-established temporary directory
        @param simulationMode: If True, prevents writing of files to storage obects or catalog
        @param writeFilePath: If not None, causes a file to be written in the given path when storage is attempted
        """
        self.storageConn = storageConn
        self.repository = repository
        self.dataSource = dataSource
        self.catalog = catalogResource
        self.tempDir = tempDir
        self.simulationMode = simulationMode
        self.writeFilePath = writeFilePath
    
    def makeFilename(self, base, ext, collectionDate):
        """
        Makes a filename from the base, ext, and collectionDate.
        """
        spanChar = "."
        if "." in ext:
            spanChar = "_"
        return base + "_" + collectionDate.strftime("%Y-%m-%d") + spanChar + ext
    
    def makePath(self, base, ext, collectionDate, filenamePart=None):
        """
        Builds a storage platform-specific path using the given base, ext, and collectionDate. Used for providing parameters
        for writing a file.
        """
        if isinstance(collectionDate, str):
            collectionDate = arrow.get(collectionDate)
        if not filenamePart:
            filenamePart = self.makeFilename(base, ext, collectionDate) 
        return self.storageConn.makePath(self.dataSource, collectionDate, filenamePart)
    
    def retrieveFilePath(self, path, destPath=None, deriveFilename=False):
        """
        retrieveFilePath(path) retrieves a resource at the given storage platform-specific path (presumably retrieved from the
        catalog) and returns a full path to the written file.
        
        @param path: The complete target platform-dependent path including the desired filename
        @param destPath: The path to write the file to; set this to null in order to write the file to the temp directory
        @param deriveFilename: Set this to false if the filename is already bundled in the destPath
        """
        if not destPath:
            destPath = self.tempDir
            deriveFilename = True
        return self.storageConn.retrieveFilePath(path, destPath=destPath, deriveFilename=deriveFilename)
    
    def retrieveJSON(self, path):
        """
        retrieveJSON(path) efficiently returns a dictionary representing JSON via a temporary file.
        """
        ret = None
        tempFilePath = tempfile.mktemp()
        if self.retrieveFilePath(path, destPath=tempFilePath):
            with open(tempFilePath, "r") as fileObj:
                ret = json.load(fileObj)
        os.remove(tempFilePath)
        return ret
    
    def retrieveBuffer(self, path):
        """
        retrieveBufferPath retrieves a resource at the given storage platform-specific path and provides it as a buffer.
        """
        return self.storageConn.retrieveBufferPath(path)
        
    def writeFile(self, sourceFile, catalogElement, cacheCatalogFlag=False):
        """
        writeFile copies a file into the resource.
        
        @param sourceFile: The full path to a file, or an open file object.
        @param catalogElement: A catalog element, which is updated to be relevant to this storage object.
        @param cacheCatalogFlag defers writing of contents to the catalog until flushCatalog() is called.
        """
        with open(sourceFile, "rb") as fileObject:
            self.writeBuffer(fileObject, catalogElement, cacheCatalogFlag=cacheCatalogFlag)
            
    def writeJSON(self, sourceJSON, catalogElement, cacheCatalogFlag=False):
        """
        writeJSON writes stringified JSON to the resource, streaming out to a temporary file to reduce RAM footprint
        """
        tempFilePath = tempfile.mktemp()
        with open(tempFilePath, "w") as outFile:
            json.dump(sourceJSON, outFile)
        self.writeFile(tempFilePath, catalogElement, cacheCatalogFlag)
        os.remove(tempFilePath)
        
    def writeBuffer(self, sourceBuffer, catalogElement, cacheCatalogFlag=False):
        """
        writeBuffer writes the contents of the buffer into the resource specified by catalogObject (base, ext, etc.) until flushCatalog() is called.
        @param sourceFile: The full path to a file, or an open file object.
        @param catalogElement: A catalog element, which is updated to be relevant to this storage object.
        @param cacheCatalogFlag defers writing of contents to the catalog until flushCatalog() is called.
        """
        newCatalogElement = self.createCatalogElement(
            catalogElement["id_base"],
            catalogElement["id_ext"],
            catalogElement["collection_date"],
            processingDate=catalogElement["processing_date"] if "processing_date" in catalogElement else None,
            metadata=catalogElement["metadata"] if "metadata" in catalogElement else None
        )
        if self.writeFilePath:
            # We have a debug flag for writing out the file to a given path. Write it.
            filename = self.makeFilename(catalogElement["id_base"], catalogElement["id_ext"], arrow.get(catalogElement["collection_date"]))
            debugPath = os.path.join(self.writeFilePath, filename)
            with open(debugPath, "wb") as outFile:
                writeFromBinBuffer(sourceBuffer, outFile)
                outFile.flush()
            if not self.simulationMode:
                # Use that written file to write to the storage repository.
                self.storageConn.writeFile(debugPath, newCatalogElement["pointer"])
            else:
                print("Simulation mode: skipped writing file '%s' to repository: '%s'" % (debugPath, newCatalogElement["pointer"]))
        else:
            if not self.simulationMode:
                # Write the given buffer to the storage repository.
                self.storageConn.writeBuffer(sourceBuffer, newCatalogElement["pointer"])
            else:
                print("Simulation mode: skipped writing buffer to repository: '%s'" % newCatalogElement["pointer"])
        if not self.simulationMode and self.catalog:
            # Add this new entry to the target catalog:
            if cacheCatalogFlag:
                self.catalog.stageUpsert(newCatalogElement)
            else:
                self.catalog.upsert(newCatalogElement)
        
    def flushCatalog(self):
        """
        flushCatalog writes all catalog contents.
        """
        if self.catalog:
            self.catalog.commitUpsert()
            
    def copyFile(self, catalogElement, tgtStorage, cacheCatalogFlag=False):
        """
        Copies the specific file to the target storage object. Requires the application object to have created a temp directory.
        """
        # Read and write the file:
        tempFilePath = self.retrieveFilePath(catalogElement["pointer"], self.tempDir, inferFilename=True)
        tgtStorage.writeFile(tempFilePath, catalogElement, cacheCatalogFlag=cacheCatalogFlag)
        
        # Clean up:
        if os.path.exists(tempFilePath):
            os.remove(tempFilePath)
        
    def catalogLookup(self, base, ext, collectionDate):
        """
        Returns a single catalog element based upon the given criteria.
        """
        return self.catalog.querySingle(self.repository, base, ext, collectionDate)
        
    def createCatalogElement(self, base, ext, collectionDate, processingDate=None, metadata=None):
        """
        Creates a catalog object based upon the given criteria. Used for providing parameters for writing a file.
        """
        return self.catalog.buildCatalogElement(self.repository, base, ext, collectionDate, processingDate, \
            self.makePath(base, ext, collectionDate), metadata)

BIN_BUFFER_SIZE = 1024
"Used in the writeFromBinBuffer() function."

def writeFromBinBuffer(readBuffer, writeBuffer):
    """
    Reads from a buffer to another buffer. Lifted off of
    https://stackoverflow.com/questions/16630789/python-writing-binary-files-bytes 
    """
    while True:
        buf = readBuffer.read(BIN_BUFFER_SIZE)
        if buf: 
            writeBuffer.write(buf)
        else:
            break

class StorageImpl:
    """
    Implements storage access functions for a specific storage platform
    """    
    def makePath(self, dataSource, collectionDate, filename=None):
        """
        Builds a storage path for the target platform using the given collectionDate and filename.
        
        @param filename: If this is supplied, then the path will include the filename.
        """
        raise NotImplementedError
    
    def extractFilename(self, path):
        """
        Extracts the filename from the given path.
        """
        raise NotImplementedError
    
    def retrieveFilePath(self, path, destPath=".", deriveFilename=False):
        """
        retrieveFilePath(path) retrieves a resource at the given storage platform-specific path (presumably retrieved from the
        catalog) and returns a full path to the written file.
        
        @param path: The target platform-dependent path in the bucket that's labeled as "repository" in this object.
        @param destPath: A path to write the file to; otherwise, the temp directory will be used. May include a filename if destFilename is None.
        @param deriveFilename: If true, obtains the filename from the given target platform path.
        """
        raise NotImplementedError
        
    def retrieveBufferPath(self, path):
        """
        retrieveBufferPath retrieves a resource at the given storage platform-specific path and provides it as a buffer.
        """
        raise NotImplementedError
        
    def writeFile(self, sourceFile, path):
        """
        writeFile writes sourceFile to the target fully specified target platform-dependent path.
        """
        raise NotImplementedError
        
    def writeBuffer(self, sourceBuffer, path):
        """
        writeBuffer writes the contents of the buffer into the target fully specified target platform-dependent path.
        """
        raise NotImplementedError
