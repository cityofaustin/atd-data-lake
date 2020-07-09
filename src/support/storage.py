"""
storage.py: Facilitates common Data Lake storage functions, with tracking with catalog

Kenneth Perrine
Center for Transportation Research, The University of Texas at Austin
"""
import os
import arrow

class Storage:
    """
    Facilitates the storage or retrieval of files within a cloud service or local volume
    """
    
    def __init__(self, app, purpose):
        """
        Initializes storage connection using the application object
        """
        self.storageConn, self.repository = app.getStorageResource(purpose)
        self.dataSource = app.dataSource
        self.catalog = app.getCatalogResource()
        self.tempDir = app.tempDir
        self.simulationMode = app.simulationMode
        self.writeFilePath = app.writeFilePath
    
    def makePath(self, base, ext, collectionDate, filenamePart=None):
        """
        Builds a storage platform-specific path using the given base, ext, and collectionDate. Used for providing parameters
        for writing a file.
        """
        if isinstance(collectionDate, str):
            collectionDate = arrow.get(collectionDate)
        if not filenamePart:
            filenamePart = base + "_" + collectionDate.strftime("%Y-%m-%d") + "." + ext 
        return self.storageConn.makePath(base, ext, collectionDate, filenamePart)
    
    def retrieveFilePath(self, path, destPath=None, inferFilename=False):
        """
        retrieveFilePath(path) retrieves a resource at the given storage platform-specific path (presumably retrieved from the
        catalog) and returns a full path to the written file.
        
        @param path: The complete S3 path including the desired filename
        @param destPath: The path to write the file to; set this to null in order to write the file to the temp directory
        @param inferFilename: Set this to false if the filename is already bundled in the destPath
        """
        if not destPath:
            destPath = self.tempDir
            inferFilename = True
        return self.dataSource.retrieveFilePath(path, destPath=destPath, inferFilename=inferFilename)
            
    def retrieveBuffer(self, path):
        """
        retrieveBufferPath retrieves a resource at the given storage platform-specific path and provides it as a buffer.
        """
        return self.dataSource.retrieveBufferPath(path)
        
    def writeFile(self, sourceFile, catalogElement, cacheCatalogFlag=False):
        """
        writeFile copies a file into the resource.
        
        @param sourceFile: The full path to a file, or an open file object.
        @param catalogElement: A catalog element, which is updated to be relevant to this storage object.
        @param cacheCatalogFlag defers writing of contents to the catalog until flushCatalog() is called.
        """
        with open(sourceFile, "rb") as fileObject:
            self.writeBuffer(fileObject, catalogElement, cacheCatalogFlag=cacheCatalogFlag)
        
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
        newCatalogElement["path"] = self.storageConn.makePath(self, catalogElement["id_base"], catalogElement["id_ext"], self.dataSource,
            catalogElement["collection_date"])
        if self.writeFilePath:
            # We have a debug flag for writing out the file to a given path. Write it.
            filename = self.makePath(catalogElement["id_base"], catalogElement["id_ext"], catalogElement["collection_date"])
            debugPath = os.path.join(self.writeFilePath, filename)
            with open(debugPath, "wb") as outFile:
                outFile.write(sourceBuffer)
                outFile.flush()
            if not self.simulationMode:
                # Use that written file to write to the storage repository.
                self.storageConn.writeFile(debugPath, newCatalogElement["path"])
            else:
                print("Simulation mode: skipped writing file '%s' to repository: '%s'" % (debugPath, newCatalogElement["path"]))
        else:
            if not self.simulationMode:
                # Write the given buffer to the storage repository.
                self.storageConn.writeBuffer(sourceBuffer, newCatalogElement["path"])
            else:
                print("Simulation mode: skipped writing buffer to repository: '%s'" % newCatalogElement["path"])
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
        tempFilePath = self.retrieveFilePath(catalogElement["path"], self.tempDir, inferFilename=True)
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
