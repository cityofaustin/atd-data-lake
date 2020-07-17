"""
Bluetooth sensor ingestion takes files from the AWAM share and places them into the Data Lake
"raw" layer.

@author Kenneth Perrine
"""
from support import app, cmdline, last_update
from config import config_app
from util import date_dirs
from drivers import last_upd_fs

# This sets up application information and command line parameters:
CMDLINE_CONFIG = cmdline.CmdLineConfig(
    appName="bt_insert_lake.py",
    appDescr="Inserts Bluetooth data from AWAM share into the Raw Data Lake",
    customArgs={("-d", "--sourcedir"): {
        "default": ".",
        "help": "Source directory (e.g. AWAM share) to read Bluetooth files from"}})

# This defines the valid filename formats that exist in the AWAM directory:
DIR_DEFS = [date_dirs.DateDirDef(prefix=config_app.UNIT_LOCATION + "_bt_",
                                 dateFormat="%m-%d-%Y",
                                 postfix=".txt"),
            date_dirs.DateDirDef(prefix=config_app.UNIT_LOCATION + "_btmatch_",
                                 dateFormat="%m-%d-%Y",
                                 postfix=".txt"),
            date_dirs.DateDirDef(prefix=config_app.UNIT_LOCATION + "_bt_summary_15_",
                                 dateFormat="%m-%d-%Y",
                                 postfix=".txt")]

class BTLastUpdateProv(last_upd_fs.LastUpdFileProv):
    """
    Overrides the default file provider so as to generate the correct identifier for AWAM files
    """
    def _getIdentifier(self, filePath, typeIndex):
        """
        Creates identifier for the comparison purposes from the given file information
        """
        # We want our identifiers to be phrased like: Base="Austin"; Ext="unmatched.txt".
        prefix = self.pattList[typeIndex].prefix.split("_")[0] # TODO: Returns "Austin"
        postfix = self.pattList[typeIndex].postfix
        if typeIndex == 0:
            desc = "unmatched"
        elif typeIndex == 1:
            desc = "matched"
        elif typeIndex == 2:
            desc = "traf_match_summary"
        else:
            raise ValueError("Bad typeIndex")
        ext = desc + postfix
        return prefix, ext

class BTInsertLakeApp(app.App):
    """
    Special behavior around Bluetooth ingestion. This may seem like overkill, but demonstrates
    how new application-specific variables can be added to the App class.
    """
    def __init__(self, args):
        """
        Initializes application-specific variables
        """
        self.sourceDir = None
        super().__init__(args, "bt",
                         purposeTgt="raw",
                         needsTempDir=False,
                         perfmetStage="Ingest")
    
    def _ingestArgs(self, args):
        """
        Processes application-specific variables
        """
        self.sourceDir = args.source_dir
        super()._ingestArgs(args)

    def etlActivity(self, processingDate):
        """
        This performs the main ETL processing.
        
        @return count: A general number of records processed
        """
        provSrc = last_upd_fs.LastUpdFileProv(self.sourceDir, DIR_DEFS)
        provTgt = self.storageTgt
        comparator = last_update.LastUpdate(provSrc, provTgt).configure(startDate=self.startDate,
                                                                        endDate=self.endDate,
                                                                        baseExtKey=True)
        for item in comparator.compare(lastRunDate=self.lastRunDate):
            
        

def main(args):
    """
    Main entry point after processing command line arguments
    """
    curApp = BTInsertLakeApp(args)
    return curApp.doMainLoop()




## Function definitions
def set_S3_pointer(filename, date, data_source='bt'): ### may have to include bucket!! ###
    # TODO: Put this in a standardized location so others can access this method.

    year = str(date.year)
    month = str(date.month)
    day = str(date.day)

    s_year = year
    s_month = month if len(month) == 2 else month.zfill(2)
    s_day = day if len(day) == 2 else day.zfill(2)

    return "{year}/{month}/{day}/{data_source}/{file}".format(year=s_year,
                                                            month=s_month,
                                                            day=s_day,
                                                            data_source=data_source,
                                                            file=filename)
def bt_metadata(repository, idBase, idExt, pointer, collectionDate):

    processing_date = str(date_util.localize(arrow.now().datetime))
    json_blob = json.dumps({"element": "True"})

    metadata = {"repository": repository, "data_source": 'bt',
               "id_base": idBase, "id_ext": idExt, "pointer": pointer,
               "collection_date": str(collectionDate),
               "processing_date": processing_date, "metadata": json_blob}

    return metadata

def bt_metadata_ingest(metadata, catalog):

    catalog.upsert(metadata)


    # Gather records of prior activity from catalog:
    print("Beginning loop...")
    count = 0
    lastUpdateWorker = LastUpdateBT(args.source_dir, "raw", monthsOld)
    for record in lastUpdateWorker.getToUpdate(lastRunDate, sameDay=args.same_day, detectMissing=args.missing):
        filename = os.path.basename(record.filePath)

        pointer = set_S3_pointer(filename=filename, date=record.fileDate)
        
        print("%s -> %s:%s%s" % (filename, BUCKET, pointer, "" if not record.missingFlag else " (missing)"))

        # Put TXT to S3:
        with open(record.filePath, 'rb') as bt_file:
            s3Object = s3.Object(BUCKET, pointer)
            s3Object.put(Body=bt_file)
        
        # Update the catalog:
        bt_metadata_ingest(bt_metadata(repository='raw', idBase=record.identifier[0], idExt=record.identifier[1],
                                      pointer=pointer, collectionDate=record.fileDate), catalog=catalog)

        # Increment count:
        count += 1
        
    print("Records processed: %d" % count)
    return count    

if __name__ == "__main__":
    """
    Entry-point when run from the command-line
    """
    args = cmdline.processArgs(CMDLINE_CONFIG)
    main(args)
