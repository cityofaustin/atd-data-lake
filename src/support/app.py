"""
app.py: Class and logic for application and driver initialization

Kenneth Perrine
Center for Transportation Research, The University of Texas at Austin
"""
import arrow

import tempfile
import shutil
from support import config
from util import date_util

"Number of months to go back for filing records"
DATE_EARLIEST = 12

class App:
    """
    App is a holding place for application-wide parameters that contain driver connection
    objects and application-wide parameters.    
    """
    def __init__(self, args, dataType, needsTempDir=True, parseDateOnly=True, needsPerfmet=True):
        """
        Constructor initializes variables.
        
        @param args: Collection of command-line arguments that were parsed with ArgumentParser or directly passed in
        @param dataType: The datatype abbreviation for application activities
        @param needsTempDir: Causes temporary directory to be managed & set to self.tempDir
        @param parseDateOnly: Causes the start/end dates to work on day boundaries only
        @param needsPerfmet: Causes the performance metrics initialization
        """
        "To be defined immediately:"
        self.dataType = dataType
        
        "To be populated in argument ingester:"
        self.startDate = None
        self.endDate = None
        self.lastRunDate = None
        self.tempDir = None
        self.productionMode = None

        "General configuration variables:"        
        self.needsTempDir = needsTempDir
        self.parseDateOnly = parseDateOnly
        self.needsPerfmet = needsPerfmet
        
        "To be populated by the connector:"
        self.catalog = None
        # TODO: Source name, target name?
        self.storage1 = None
        self.storage2 = None
        self.perfmet = None
        
        # Call the argument ingester:        
        self._ingestArgs(args)
        self.args = args
        
    def _ingestArgs(self, args):
        """
        This is where arguments are ingested and set to Initializer class-level attributes. Override
        this and call the parent if custom arguments need to be processed.
        """
        # Local time zone:
        date_util.setLocalTimezone(config.TIMEZONE)
        
        # Last run date:
        if args.last_run_date:
            self.lastRunDate = date_util.parseDate(args.last_run_date, dateOnly=True)
            print("Last run date: %s" % str(self.lastRunDate))
        else:
            self.lastRunDate = None
    
        # Start date, or number of days back:
        if not args.start_date:
            args.start_date = DATE_EARLIEST
        try:
            dateEarliest = int(args.start_date)
            self.startDate = date_util.localize(arrow.now()
                .replace(hour=0, minute=0, second=0, microsecond=0)
                .shift(days=-dateEarliest).datetime)
        except ValueError:
            self.startDate = date_util.parseDate(args.start_date, dateOnly=self.parseDateOnly)

        # End date:
        if args.end_date:
            self.endDate = date_util.parseDate(args.end_date, dateOnly=self.parseDateOnly)
        else:
            self.endDate = None
            
        # Production mode:
        self.productionMode = config.electProdMode(not self.productionMode)        
    
        # Catalog and AWS connections:
        config.setDataType(self.dataType)
    
        # Debugging features:
        self.simulationMode = args.simulate
        self.writeFilePath = args.output_filepath
        
        
        # ------------- Need to figure out.
        catalog = Postgrest(config.CATALOG_URL, auth=config.CATALOG_KEY)
        _S3 = config.getAWSSession().resource('s3')
    
        # Set up temporary output directory:
        if self.needsTempDir:
            self.tempDir = tempfile.mkdtemp()
            print("Created holding place: %s" % self.tempDir)

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
            