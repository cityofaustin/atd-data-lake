"""
publish.py coordinates the publishing of data, and noting the publishing in the catalog.

@author Kenneth Perrine
"""
import csv

class Publisher:
    """
    Coordinates the publishing of data and recording in the catalog.
    
    @param connector: The implementer of publishing, of type PublishConnBase
    """
    def __init__(self, connector, catalog, dataSource):
        """
        Initializes the object
        """
        self.connector = connector
        self.catalog = catalog
        self.dataSource = dataSource
        self.chunkSize = connector.getPreferredChunk()
        
        # For internal operation:
        self.buffer = []
        self.rowCounterPreFlush = 0
        
    def addRow(self, jsonRecord):
        """
        Adds a row to the buffer, and flushes if we get up to the chunk size.
        """
        self.buffer.append(jsonRecord)
        if self.chunkSize and len(self.buffer) >= self.chunkSize:
            self.flush()
            
    def flush(self):
        """
        Writes out the buffer contents
        """
        self.connector.write(self.buffer)
        self.rowCounterPreFlush += len(self.buffer) 
        self.buffer.clear()
        
    def reset(self):
        """
        Aborts writing of the buffer and resets the object.
        """
        self.buffer.clear()
        self.rowCounterPreFlush = 0
        
    def close(self):
        """
        Closes down the object. Makes sure everything is flushed. Dereferences the connector.
        """
        if self.buffer:
            self.flush()
        self.connector.close()
        self.connector = None

    def __delete__(self):
        """
        Automatically writes upon close-down of the object. The preferred method is to use flush().
        """
        if self.connector:
            self.close()

class PublishConnBase:
    """
    Base class for publishing
    """
    def write(self, dataRows):
        """
        Writes records to the publishing resource.
        """
        pass

    def getPreferredChunk(self):
        """
        Returns the preferred maximum number of rows to write.
        """
        return None
    
    def close(self):
        """
        Performs a close operation
        """
        pass

class PublishCSVConn(PublishConnBase):
    """
    Implements CSV file output for publishing
    """
    def __init__(self, filepath):
        """
        Initializes the object.
        
        @param filepath: The path plus filename of the CSV file to write
        """
        self.filepath = filepath
        self.fileHandle = open(filepath, 'w', newline='')
        self.csvWriter = csv.writer(self.fileHandle)
        self.header = None 
    
    def write(self, dataRows):
        """
        Writes records to the publishing resource. Header information will come out of the first row.
        """
        for row in dataRows:
            if not self.header:
                self.header = []
                for key in row:
                    self.header.append(key)
                self.csvWriter.writerow(self.header)
            rowBuffer = []
            for key in self.header:
                rowBuffer.append(row[key])
                self.csvWriter.writerow(self.rowBuffer)

    def getPreferredChunk(self):
        """
        Returns the preferred maximum number of rows to write.
        """
        return 1
    
    def close(self):
        """
        Performs a close operation
        """
        self.fileHandle.close()
