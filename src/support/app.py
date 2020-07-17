"""
app.py: Class and logic for application and driver initialization

Kenneth Perrine
Center for Transportation Research, The University of Texas at Austin
"""
import arrow

import tempfile
import shutil
import config
from util import date_util

"Number of months to go back for filing records"
DATE_EARLIEST = 365

class App:
    """
    App is a holding place for application-wide parameters that contain driver connection
    objects and application-wide parameters.    
    """
    def __init__(self, args, dataSource, purposeSrc=None, purposeTgt=None, needsTempDir=True, parseDateOnly=True, perfmetStage=None):
        """
        Constructor initializes variables.
        
        @param args: Collection of command-line arguments that were parsed with ArgumentParser or directly passed in
        @param dataSource: The data source abbreviation for application activities
        @param purposeSrc: A purpose string for the source, used to get the source repository name
        @param purposeTgt: The purpose string for the target, used to get the target repository name
        @param needsTempDir: Causes temporary directory to be managed & set to self.tempDir
        @param parseDateOnly: Causes the start/end dates to work on day boundaries only
        @param perfmetStage: Causes the performance metrics initialization to happen with the stage name if specified
        """
        "To be defined immediately:"
        self.dataSource = dataSource
        self.purposeSrc = purposeSrc
        self.purposeTgt = purposeTgt
        
        "Typical connection parameters for ETL:"
        self.catalog = None
        self.storageSrc = None
        self.storageTgt = None
        self.perfmet = None
        
        "To be populated in argument ingester:"
        self.startDate = None
        self.endDate = None
        self.lastRunDate = None
        self.tempDir = None
        self.productionMode = None

        "General configuration variables:"        
        self.needsTempDir = needsTempDir
        self.parseDateOnly = parseDateOnly
        self.perfmetStage = perfmetStage
                
        # Call the argument ingester:        
        self.args = args
        self._ingestArgs()
        self._connect()
        
    def _ingestArgs(self, args):
        """
        This is where arguments are ingested and set to Initializer class-level attributes. Override
        this and call the parent if custom arguments need to be processed.
        """
        # Local time zone:
        date_util.setLocalTimezone(config.getLocalTimezone())
        
        # Last run date:
        if args.last_run_date:
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
        if args.start_date:
            try:
                dateEarliest = int(args.start_date)
                self.startDate = date_util.localize(arrow.now()
                    .replace(hour=0, minute=0, second=0, microsecond=0)
                    .shift(days=-dateEarliest).datetime)
            except ValueError:
                self.startDate = date_util.parseDate(args.start_date, dateOnly=self.parseDateOnly)
        else:
            self.dateEarliest = None

        # End date:
        if args.end_date:
            self.endDate = date_util.parseDate(args.end_date, dateOnly=self.parseDateOnly)
        else:
            self.endDate = None
            
        # Production mode:
        self.productionMode = config.electProductionMode(not args.debugmode) \
            if hasattr(args, "debugmode") else config.electProductionMode()
    
        # Debugging features:
        self.simulationMode = args.simulate
        self.writeFilePath = args.output_filepath
            
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
        
        
        # TODO: Preparation method call?
        
        runCount = 1
        recsProcessed = 0
        # --- BEGIN STUFF. TODO: Support for loop over time period at intervals (with functional disable for that)?
        
        # TODO: Exception handling with retry ability?
        
        recsProcessed += self.etlActivity(date_util.localize(arrow.now().datetime), runCount)

        runCount += 1
        # --- END STUFF
        
        # TODO: Shutdown method call?
        
        return recsProcessed

    def etlActivity(self, processingDate, runCount):
        """
        This performs the main ETL processing, to be implemented by the specific application.
        
        @return count: A general number of records processed
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
            