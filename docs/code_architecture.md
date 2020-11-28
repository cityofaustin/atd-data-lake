# Code Architecture

*[(Back to Docs Catalog)](index.md)*

This document describes the code architecture of the "atd-data-lake" project. Specifically, this has to do with the structure of the code and processes that should be considered in efforts for expansion, and support new data types in the Data Lake. (The strucutre is also appropriate for data transformation activities that don't necessarily read and write files to and from the Data Lake.)

Recent efforts to streamline code have worked extensively to do the following:

* Capture common tasks repeated across most ETL scripts into classes and library functions. This considerably simplifies code and eases the creation of new ETL processes that support new data types.
* Abstract data, storage and publishing code with interfaces, placing all device- and platform-specific implementations into the "drivers" packages. Put most intstanciations into factory methods accessed in the "config" package. This makes it easy to support new platforms, or to change out existing platforms, reducing dependency on any one vendor's technology.
* Better organize code into packages according to entry points, support functionality, device-specific implementations, and configurations. This make expansion of code easier.
* Add command line options and functionlities to storage and publishing interfaces to read and write basic files. This helps in debugging code without relying upon or affecting third party storage or publishing resources.

For information on the data flow architecture and specifics around the individual data types, please refer to the [ATD Data Lake Architecture: Technical](tech_architecture.md) document.

## Structure

### Packages

The atd-data-lake repository "src" tree contains the following packages:

* **Root directory ".":** This contains entry-point ETL scripts that can be started from the command line. Each of these also has a `main()` function that can be called from another Python script with a set of arguments equivalent to those supported from the command line. (**TODO:** We can still move these into another location such as "entries" if that makes it easier to import scripts from other Python code.)
* **config:** Factory methods that instanciate platform-specific classes given data type are placed in the package script. Other configuration files contain public access credentials. The "config_secret.py" script contains passwords and other information that should not be stored in the repository. (**TODO:** We should still keep in mind the idea of transitioning to an online password broker solution.)
* **drivers** and **drivers.devices:** This is where platform or device-specific code goes. Most of these are coded to an interface. Conceivably, if one were to code support for another platform to the same interface, and use the new interface in the "config" factory methods, the new platform would be fully supported.
* **support:** The core classes and interfaces that comprise the ETL functionalities exist in this package, including the "etl_app.py" script, which contains the central command line parsing and main loop processing class, `ETLApp`.
* **util:** Support modules that help with file and date/time processing

### Processing

As mentioned above, the main processing loop is facilitated by the `ETLApp` class found in "etl_app.py". When an ETL script sets up the use of `ETLApp`, it generally follows the structure depicted below. These are the assumptions:

* The ETL process is called "my_app"
* The abbreviated data type is called "zz". Note that if this were to be supported, "zz" must be handled within the "config" package factory methods.
* The source "purpose" is "srcPurpose" and the target "purpose" is "tgtPurpose". These are tags that help "config" factory methods assign the correct classes.
* The perfmet stage identifier is "myStage". This causes perfmet database entries to be labeled as such.

```python
APP_DESCRIPTION = etl_app.AppDescription(
    appName="my_app.py",
    appDescr="Description of ETL app")

class MyApp(etl_app.ETLApp):
    """
    Application functions and special behavior around ingestion for data source zz.
    """
    def __init__(self, args):
        """
        Initializes application-specific variables
        """
        super().__init__("zz", APP_DESCRIPTION,
                         args=args,
                         purposeSrc="srcPurpose",
                         purposeTgt="tgtPurpose",
                         perfmetStage="myStage")
        self.customField1 = None
        self.customField2 = None

    def _addCustomArgs(self, parser):
        """
        Override this and call parser.add_argument() to add custom command-line arguments.
        """
        parser.add_argument("-y", "--ycustom", help="Custom parameter support")
        
    def _ingestArgs(self, args):
        """
        Processes application-specific variables
        """
        self.customField2 = args.ycustom
        super()._ingestArgs(args)
    
    def etlActivity(self):
        """
        This performs the main ETL processing.
        
        @return count: A general number of records processed
        """
        # First, get the unit data for Wavetronix:
        unitDataProv = config.createUnitDataAccessor(self.dataSource)
        self.unitData = unitDataProv.retrieve()
                
        # Configure the source and target repositories and start the compare loop:
        count = self.doCompareLoop(last_update.LastUpdStorageCatProv(self.storageSrc),
                                   last_update.LastUpdStorageCatProv(self.storageTgt),
                                   baseExtKey=False)
        print("Records processed: %d" % count)
        return count    

    def innerLoopActivity(self, item):
        """
        This is where the actual ETL activity is called for the given compare item.
        """
        # Write unit data to the target repository:
        if self.itemCount == 0:
            config.createUnitDataAccessor(self.storageTgt).store(self.unitData)
            
        # Read in the file and call the transformation code.
        print("%s: %s -> %s" % (item.label, self.storageSrc.repository, self.storageTgt.repository))
        filepathSrc = self.storageSrc.retrieveFilePath(item.label)

        # (Often, the actual file read and ETL processing is put into a function call)
        outJSON = {}
        with open(filepathSrc, "rt") as fileReader:
            "Do ETL activity and fill out outJSON here."

        # Clean up:
        os.remove(filepathSrc)
        
        # Prepare for writing to the target:
        catalogElement = self.storageTgt.createCatalogElement(item.identifier.base, "json",
                                                              item.identifier.date, self.processingDate)
        self.storageTgt.writeJSON(outJSON, catalogElement)
            
        # We can do other things such as write performance metrics here.
        self.perfmet.recordCollect(item.identifier.date, representsDay=True)
        return 1

def main(args=None):
    """
    Main entry point. Allows for dictionary to bypass default command-line processing.
    """
    curApp = MyApp(args)
    return curApp.doMainLoop()

if __name__ == "__main__":
    """
    Entry-point when run from the command-line
    """
    main()
```

This is what each part does, listed in the order of first encounter when running:

* **The module:** Calls the `main()` function with no arguments, which allows `ETLApp` to read all arguments from the command line.
* **main():** Instanciates the `ETLApp` subclass `MyApp`.
* **\_\_init\_\_():** The subclass allows me to nicely pass in standardized parameters for `ETLApp` that are specific to my ETL process, and also allows me to have custom application-wide class attributes `customField1` and `customField2`.
* **_addCustomArgs()** and **_ingestArgs():** Put processing and initialization of any special command-line or `main()` arguments here. These are called from `ETLApp`. If there are no custom parameters, then these methods can be omitted.
* **ETLApp.doMainLoop():** This hands over control to my instance of ETLApp subclass, which then calls my `etlActivity()`. (**TODO:** It is in here that further initialization code, benchmarking, and exception handling with retry code can be added).
* **etlActivity():** Sets up and hands control over to the main processing loop (via `ETLApp.doCompareLoop()`), which needs "source" and a "target" data providers. Here, these are both storage repositories that are paired with catalog entries, but could be devices, publishers, etc. The number of items processed should be returned here.
* **innerLoopActivity():** For each item that the compare loop finds that needs to be processed, this method is called. The `item` parameter, a `support.last_update._LastUpdateItem`, will always be identical or incrementing in date each time `innerLoopActivity()` is called. This is where retrieval of resources, transformation, and writing of resources is to happen. The number of items processed in this call should be returned here.

Of special interest is `support.last_update._LastUpdateItem`, which is the type of the parameter that is passsed into `innerLoopActivity()`:

* **identifier:** This tuple, `base`, `ext`, and `date` identifies an item stored within a repositry for a data source.
* **priorLastUpdate:** This is set to `True` if the item comes from a date that precedes the LastRunDate (as passed in on the command line). It's a way of knowing if something is being updated that happens before the expected update time.
* **provItem:** This comes from the source data provider-- a `last_update.LastUpdProv._LastUpdProvItem`, which contains a `payload` attribute that is specific to the data source.
* **label:** This is a string representation of the data item, usually the item's filename.

### Other ETLApp Characteristics

#### Constructor

When `ETLApp` is constructed, these parameters are to be passed in:

* **dataSource:** The two-letter abbreviation for the data type or source being processed. Required.
* **appDescription:** An `etl_app.AppDescription` object that provides "about" information that shows up in the command line help. Required.
* **args:** If this is `None` (default), then the command line will be processed; otherwise, a dictionary needs to be passed in that contains all of the contents that would otherwise occur on the command line, keyed as the long command line argument names.
* **purposeSrc**, **purposeTgt:** If this is provided, then a storage object is attempted to be created using the `config.createStorage()` factory method.
* **needsTempDir:** If `False`, prevents a temporary directory from being created.
* **parseDateOnly:** This should be `True` if the data source is assumed to be provided and processed on a daily or nightly basis; otherwise, it is assumed that the ETL process is run multiple times throughout the day.
* **perfmetStage:** This string identifies the performance metrics stage that is to be associated with this ETL process; if `None` is provided, then no PerfMet object is created.

#### Attributes

`ETLApp` maintains a number of attributes that are to be used by the implementation:

* **dataSource**, **purposeSrc**, and **purposeTgt:** These come from initialization, which is the two-letter data source code and purpose strings that are used in "config" factory methods that help point to the desired repository.
* **catalog:** A `catalog.Catalog` object that represents a connection to a data source's catalog.
* **storageSrc**, **storageTgt:** A `storage.Storage` object that is set to connect to the source and target storage; `None` if not set up.
* **perfmet:** A `perfmet.PerfMet` object that receives and maintains data health performance metrics.
* **startDate**, **endDate**, and **lastRunDate:** These come from command line arguments and configure the main compare loop.
* **forceOverwrite**, **productionMode**, **simulationMode**, and **writeFilePath:** These come from the command line and set various options for the compare loop and storage classes.
* **tempDir:** This is set up when `ETLApp` is in its initialization, and is deleted when the ETL application exits. It is meant to be used as a temporary holding-place.

At the time `etlActivity()` is called, these `ETLApp` attributes are set to the following:

* **processingDate:** This is set to the timestamp for when `doMainLoop()` started running.
* **runCount:** Currently, this is always `1`, but when outer exception handling and retry code is added, this will increment when retries are attempted.

At the time `innerLoopActivity()` is called, these `ETLApp` attributes are available:

* **processingDate:** This is updated for each time the loop in `doCompareLoop()` is iterated.
* **itemCount:** This is incremented with the return from my `innerLoopActivity()`, which should represent the number of ETL items processed so far.
* **prevDate:** This is the previous date that was processed during the last time `innerLoopActivity()` was called. It is then possible inside `innerLoopActivity()` to see if we arrived at a new day by comparing `item.identifier.date` with `prevDate`.

### Storage

The Storage class manages the reading and writing of data items to and from an implemented resource (implemented by interface `StorageImpl`). The one that exists right now is `drivers.storage_s3.StorageS3`. One normally doesn't need to interact with `StorageImpl` directly; instead, access storage using these methods provided by `Storage`, which is usually created with the config factory method `config.createStorage()` (see the code for more documentation):

* **The constructor:** This is called from the config factory method `config.createStorage()`, but gives you options for writing files locally while writing to the repository, and also simulating writing by suppressing writes to the repository. Supplying a local path and enabling simulation mode will write files locally but not to the repository.
* **retrieveFilePath():** Retrieves a resource at the given storage platform-specific path. While you can use `makePath()` to create one fron scratch, you could be getting the resource path from the catalog for an existing item. (Minimally, `catalogLookup()` can be used to retrieve a catalog entry from the catalog, and the `pointer` member has the path). This returns a full path to the written file after the file has been retrieved.
* **retrieveJSON():** This does a similar thing, but returns a JSON dictionary that had been efficiently created via a temporary file.
* **retrieveBuffer():** Same for a buffer.
* **writeFile()**, **writeJSON()**, and **writeBuffer():** These are like the "retrieve" counterparts; however, a catalog element (which is a dictionary keyed according to a catalog entry) is passed in; use `createCatalogElement()` to make one, unless you already have one on hand from a previous query to the catalog. Also, if `cacheCatalogFlag` is `True`, the update of the catalog can be cached until `flushCatalog()` is called, which can slightly speed up operations or ensure that a set of files are uploaded before recording the entries.
* **copyFile():** This is a convenience function for copying a file from one repository to another.

### Catalog

The Catalog class manages access to a catalog that is implemented through an abstracted way-- a "driver". The current support is for PostgREST, but this could be replaced with direct database access for any platform. See the `support.catalog` module for documentation on calls that are made to query and write to the catalog.

There are a variety of calls for querying the catalog, and also efficiently searching through catalog entries that have already been retrieved through the driver. The "vehicle" for catalog information mirrors the database structure in the current PostgreSQL/PostgREST implementation (as seen in `buildCatalogElement()`). (The database structure and column definitions are available in the [Technical Architecture](tech_architecture.md))

Of special notes are "upserts" of the catalog. If an upsert call is made, then if an entry already exists in the catalog that shares the same data source, repository, id_base, id_ext, and collection_date, then the remaining contents are updated; otherwise a new entry is created.

### Last Update

The "last_update" code is responsible for iterating through the contents of some kind of data source and identifying which items exist. This is then used in the main compare loop to determine which days' worth of data must be retreived from the source data so that the target data can be updated. If data already exists in the target (usually as evidenced by the catalog), then the respective available source data is skipped unless the "force" option is used.

The core that runs "last_update" is in the `support.last_update` module, using "drivers" that implement the `support.last_update.LastUpdProv` interface. A commonly used one is `LastUpdCatProv`, which allows the main compare loop to use the catalog to determine which days of data need to be updated. There is also the `LastUpdDB` class that is a generalized adaptor for data stored within a database. An example of a database class implementation is `drivers.devices.wt_mssql_db.WT_MSSQL_DB`, which queries the MS SQL database that hosts Wavetronix data. That's instanciated directly from "wt_insert_lake.py".

### Unit Data

A concept that occurs quite frequently in the ETL processing is "Unit Data"-- that is, a file that contains records that serve as descriptors or metadata for devices that provide data. In the "Ready" stage of ETL processing, individual records from the Unit Data are placed within the data JSON files such that each JSON file is self-describing and doesn't rely upon metadata contained in another file. In City of Austin, the Unit Data comes from the Knack inventory of city assets. The UnitData class handles the retrieval of these data.

### Perfmet

Basic statistics are gathered in the JSON Standardization stage on the number of nodes or locations are up and running, as well as a basic statistic that is generated from each of those locations. These are then fed to Knack and then visualized. The motivation for collecting these data is to allow the general health of sensors for a data source to be quickly monitored, answering the following:

* Which sensors are responsive?
* Do the sensors appear to be generating data?
* Are the data obviously faulty? (e.g. all zeros or all high in comparison with a moving average over the last few days)

More about performance metrics is found in the [Performance Metrics Appendix](appendix_perfmet.md).

### Publishing

Publishing is handled in `publish.Publisher`, which uses a connector to a subclass of `PublishConnBase`. The implementation mostly used by ATD Data Lake is a driver for Socrata, found at `drivers.publish_socrata.PublishSocrataConn`. There is also `publish.CSVConn` that can be used as a fallback (or output during simulation), which produces CSV files of the data that are being sent to the Publisher.

While the ETL script is publishing, the Publisher object buffers rows of data until `flush()` is called. At that time, data are transfered to the connector class. As implemented, data are sent to Socrata in 10,000-row chunks.
