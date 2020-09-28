# Code Architecture

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
* **config:** Factory methods that instanciate platform-specific classes given data type are placed in the package script. Other configuration files contain public access credentials. The "config_secret.py" script contains passwords and other information that should not be stored in the repository. (**TODO:** We should stil keep in mind the idea of transitioning to an online password broker solution.)
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
* **__init__():** The subclass allows me to nicely pass in standardized parameters for `ETLApp` that are specific to my ETL process, and also allows me to have custom application-wide class attributes `customField1` and `customField2`.
* **_addCustomArgs()** and **_ingestArgs():** Put processing and initialization of any special command-line or `main()` arguments here. These are called from `ETLApp`. If there are no custom parameters, then these methods can be omitted.
* **ETLApp.doMainLoop():** This hands over control to my instance of ETLApp subclass, which then calls my `etlActivity()`. (**TODO:** It is in here that further initialization code, benchmarking, and exception handling with retry code can be added).
* **etlActivity():** Sets up and hands control over to the main processing loop (via `ETLApp.doCompareLoop()`), which needs "source" and a "target" data providers. Here, these are both storage repositories that are paired with catalog entries, but could be devices, publishers, etc. The number of items processed should be returned here.
* **innerLoopActivity():** For each item that the compare loop finds that needs to be processed, this method is called. The `item` parameter, a `support.last_update._LastUpdateItem`, will always be identical or incrementing in date each time `innerLoopActivity()` is called. This is where retrieval of resources, transformation, and writing of resources is to happen. The number of items processed in this call should be returned here.

Of special interest is `support.last_update._LastUpdateItem`, which is the type of the parameter that is passsed into `innerLoopActivity()`:

* **identifier:** 
* **priorLastUpdate:** 
* **provItem:** 
* **label:** 

`ETLApp` maintains a number of attributes that are to be used by the implementation:

* **dataSource:** 
* **purposeSrc:** 
* **purposeTgt:** 
* **catalog:** 
* **storageSrc:** 
* **storageTgt:** 
* **perfmet:** 
* **startDate:** 
* **endDate:** 
* **lastRunDate:** 
* **forceOverwrite:** 
* **tempDir:** 
* **productionMode:** 
* **simulationMode:** 
* **writeFilePath:** 

At the time `etlActivity()` is called, these `ETLApp` attributes are set to the following:

* **processingDate:** This is set to the timestamp for when `doMainLoop()` started running.
* **runCount:** Currently, this is always `1`, but when outer exception handling and retry code is added, this will increment when retries are attempted.

At the time `innerLoopActivity()` is called, these `ETLApp` attributes are available:

* **processingDate:** This is updated for each time the loop in `doCompareLoop()` is iterated.
* **itemCount:** This is incremented with the return from my `innerLoopActivity()`, which should represent the number of ETL items processed so far.
* **prevDate:** This is the previous date that was processed during the last time `innerLoopActivity()` was called. It is then possible inside `innerLoopActivity()` to see if we arrived at a new day by comparing `item.identifier.date` with `prevDate`.


### Abstractions


## Theory of Operation