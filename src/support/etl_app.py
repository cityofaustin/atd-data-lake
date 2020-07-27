"""
etl_app.py: Class and logic for application and driver initialization

Kenneth Perrine
Center for Transportation Research, The University of Texas at Austin
"""
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from collections import namedtuple

import arrow

import tempfile
import shutil
import config
from util import date_util
from support import last_update

# TODO: Determine if we want to limit by certain number of days if no lower bound, or throw an exception.
#"Number of months to go back for filing records"
#DATE_EARLIEST = 365

"""
AppDescription:
appName: The name of the application (short string)
appDescr: Description of the application (longer string)
"""
AppDescription = namedtuple("CmdLineConfig", "appName appDescr")

class ETLApp:
    """
    ETLApp is a holding place for application-wide parameters that contain driver connection
    objects and application-wide parameters.    
    """
    def __init__(self, dataSource, appDescription, args=None, purposeSrc=None, purposeTgt=None, needsTempDir=True, parseDateOnly=True, perfmetStage=None):
        """
        Constructor initializes variables.
        
        @param args: Collection of command-line arguments; use None to allow the default command line to be parsed
        @param dataSource: The data source abbreviation for application activities
        @param purposeSrc: A purpose string for the source, used to get the source repository name
        @param purposeTgt: The purpose string for the target, used to get the target repository name
        @param needsTempDir: Causes temporary directory to be managed & set to self.tempDir
        @param parseDateOnly: Causes the start/end dates to work on day boundaries only
        @param perfmetStage: Causes the performance metrics initialization to happen with the stage name if specified
        """
        # To be defined immediately:
        self.dataSource = dataSource
        self.purposeSrc = purposeSrc
        self.purposeTgt = purposeTgt
        
        # Typical connection parameters for ETL:
        self.catalog = None
        self.storageSrc = None
        self.storageTgt = None
        self.perfmet = None
        
        # To be populated in argument ingester:
        self.startDate = None
        self.endDate = None
        self.lastRunDate = None
        self.forceOverwrite = False
        self.tempDir = None
        self.productionMode = None
        self.simulationMode = False
        self.writeFilePath = None

        # General configuration variables:        
        self.needsTempDir = needsTempDir
        self.parseDateOnly = parseDateOnly
        self.perfmetStage = perfmetStage
        
        # State while the inner loop is running:
        self.processingDate = None
        self.runCount = 0
        self.itemCount = 0
        self.prevDate = None
                
        # Parse the command line:
        if not args:
            args = self.processArgs(appDescription)
        
        # Call the argument ingester:        
        self.args = args
        self._ingestArgs(args)
        self._connect()

    def processArgs(self, cmdLineConfig):
        """
        Builds up the command line processor with standard parameters and also custom parameters that are passed in.
        """
        parser = ArgumentParser(prog=cmdLineConfig.appName,
                                description=cmdLineConfig.appDescr,
                                formatter_class=RawDescriptionHelpFormatter)
        # Tier-1 parameters:
        parser.add_argument("-r", "--last_run_date", help="last run date, in YYYY-MM-DD format with optional time zone offset")
        parser.add_argument("-s", "--start_date", help="start date; process no more than this number of days old, or provide YYYY-MM-DD for absolute date")
        parser.add_argument("-e", "--end_date", help="end date; process no later than this date, in YYYY-MM-DD format")
        
        # Custom parameters:
        self._addCustomArgs(parser)
        
        # Tier-2 parameters:
        parser.add_argument("-F", "--force", action="store_true", help="force overwrite of records regardless of history")
        parser.add_argument("-o", "--output_filepath", help="specify a path to output files to a specific directory")
        parser.add_argument("-0", "--simulate", action="store_true", help="simulates the writing of files to the filestore and catalog")
        # TODO: Enable the logging features.
        #parser.add_argument("-L", "--logfile", help="enables logfile output to the given path")
        #parser.add_argument("--log_autoname", help="automatically create the log name from app parameters")
        parser.add_argument("--debug", action="store_true", help="sets the code to run in debug mode, which usually causes access to non-production storage")
        
        # TODO: Consider parameters for writing out files?
        args = parser.parse_args()
        return args
    
    def _addCustomArgs(self, parser):
        """
        Override this and call parser.add_argument() to add custom command-line arguments.
        """
        pass

    def _ingestArgs(self, args):
        """
        This is where arguments are ingested and set to Initializer class-level attributes. Override
        this and call the parent if custom arguments need to be processed.
        """
        # Local time zone:
        date_util.setLocalTimezone(config.getLocalTimezone())
        
        # Last run date:
        if hasattr(args, "last_run_date") and args.last_run_date:
            try:
                lastRunDate = int(args.last_run_date)
                self.lastRunDate = date_util.localize(arrow.now()
                    .replace(hour=0, minute=0, second=0, microsecond=0)
                    .shift(days=-lastRunDate).datetime)
            except ValueError:
                self.lastRunDate = date_util.parseDate(args.last_run_date, dateOnly=self.parseDateOnly)
            print("Last run date: %s" % str(self.lastRunDate))
        else:
            self.lastRunDate = None
    
        # Start date, or number of days back:
        if hasattr(args, "start_date") and args.start_date:
            try:
                dateEarliest = int(args.start_date)
                self.startDate = date_util.localize(arrow.now()
                    .replace(hour=0, minute=0, second=0, microsecond=0)
                    .shift(days=-dateEarliest).datetime)
            except ValueError:
                self.startDate = date_util.parseDate(args.start_date, dateOnly=self.parseDateOnly)
        else:
            self.startDate = None

        # End date:
        if hasattr(args, "end_date") and args.end_date:
            self.endDate = date_util.parseDate(args.end_date, dateOnly=self.parseDateOnly)
        else:
            self.endDate = None
            
        if self.startDate or self.endDate:
            dateStr = "INFO: Processing time range:"
            if self.startDate:
                dateStr += " " + str(self.startDate)
                if self.endDate:
                    dateStr += " up to " + str(self.endDate)
                else:
                    dateStr += " onward"
            print(dateStr + ".")
        if not self.lastRunDate and not self.startDate:
            raise Exception("A last_run_date or start_date must be specified.")
            
        # Force overwrite:
        if hasattr(args, "force"):
            self.forceOverwrite = args.force
            if self.forceOverwrite:
                print("INFO: Force mode is on: items will be overwritten.") 
            
        # Production mode:
        self.productionMode = config.electProductionMode(not args.debug) \
            if hasattr(args, "debug") else config.electProductionMode()
        if not self.productionMode:
            print("INFO: Debug mode is enabled.")
    
        # Debugging features:
        if hasattr(args, "simulate"):
            self.simulationMode = args.simulate
            if self.simulationMode:
                print("INFO: Simulated write mode is enabled.")
        if hasattr(args, "output_filepath"):
            self.writeFilePath = args.output_filepath
            if self.writeFilePath:
                print("INFO: Write file path is %s." % self.writeFilePath)
            
        # Set up temporary output directory:
        if self.needsTempDir:
            self.tempDir = tempfile.mkdtemp()
            print("Created holding place: %s" % self.tempDir)
            
    def _connect(self):
        """
        Establishes connections typical for an ETL process
        """
        # Establish the catalog connection:
        self.catalog = config.createCatalog(self.dataSource)
        
        # Establish the source and target storage resources:
        if self.purposeSrc:
            self.storageSrc = config.createStorage(self.catalog, self.purposeSrc, self.dataSource,
                                                   tempDir=self.tempDir,
                                                   simulationMode=self.simulationMode,
                                                   writeFilePath=self.writeFilePath)
        if self.purposeTgt:
            self.storageTgt = config.createStorage(self.catalog, self.purposeTgt, self.dataSource,
                                                   tempDir=self.tempDir,
                                                   simulationMode=self.simulationMode,
                                                   writeFilePath=self.writeFilePath)

        # Establish performance metrics:
        if self.perfmetStage:
            self.perfmet = config.createPerfmet(self.perfmetStage, self.dataSource)

    def doMainLoop(self):
        """
        Coordinates the main loop activity
        """
        # TODO: Add in benchmarking
        
        # TODO: Add in a preparation method call?
        
        # --- BEGIN STUFF. TODO: Support for loop over time period at intervals (with functional disable for that)?
        self.runCount = 1
        recsProcessed = 0
        self.processingDate = date_util.localize(arrow.now().datetime)
        
        # TODO: Exception handling with retry ability?
        
        recsProcessed += self.etlActivity()

        self.runCount += 1
        # --- END STUFF
        
        if self.perfmet:
            self.perfmet.logJob(recsProcessed)
        
        # TODO: Shutdown method call?
        
        return recsProcessed

    def etlActivity(self):
        """
        This performs the main ETL processing, to be implemented by the specific application.
        
        @return count: A general number of records processed
        """
        return 0
    
    def doCompareLoop(self, provSrc, provTgt, baseExtKey=True):
        """
        Sets up and iterates through the compare loop, calling innerLoopActivity.
        
        @param provSrc: Specifies source providers as a last_update.LastUpdateProv object
        @param provTgt: Specifies the target provider as a last_update.LastUpdateProv object, or None for all sources
        """
        comparator = last_update.LastUpdate(provSrc, provTgt,
                                force=self.forceOverwrite).configure(startDate=self.startDate,
                                                                     endDate=self.endDate,
                                                                     baseExtKey=baseExtKey)
        self.itemCount = 0
        self.prevDate = None
        for item in comparator.compare(lastRunDate=self.lastRunDate):
            if item.identifier.date != self.prevDate and self.storageTgt:
                self.storageTgt.flushCatalog()
            
            countIncr = self.innerLoopActivity(item)
            
            if countIncr:
                if not self.prevDate:
                    self.prevDate = item.identifier.date
            self.itemCount += countIncr            
        else:
            self.storageTgt.flushCatalog()
        return self.itemCount
        
    def innerLoopActivity(self, item):
        """
        This is where the actual ETL activity is called for the given compare item.
        
        Handy class attributes for helping processing are:
            self.processingDate: Identifies the current processing date
            self.itemCount: Starting with 0, this is the number of items that have been processed (increments with return value)
            self.prevDate: This can be used to compare against item.date to see if processing has shifted to a new date
        
        @param item: Type last_update.LastUpdProv._LastUpdProvItem that describes the item to be updated
        @return Number of items that were updated
        """
        return 0

    def __delete__(self):
        """
        The temporary directory is deleted when the object comes to an end.
        """
        if self.tempDir:
            try:
                shutil.rmtree(self.tempDir)
            except Exception as exc:
                print("ERROR: Exception occurred in removing temporary directory '%s':" % self.tempDir)
                exc.print_stack_trace()
            self.tempDir = None
            