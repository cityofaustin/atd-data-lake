'''
gs_exportcounts retrieves GRIDSMART counts over a date range and ships the counts to the given destination

# TODO: Tie this in with the ETL database mechanism for keeping track of modification date.

@author: Kenneth Perrine
'''
from __future__ import print_function
from collecting import gs_getcounts
import datetime
import tempfile
import shutil
import sys
import os

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

PROGRAM_DESC = """gs_exportcounts retrieves GRIDSMART counts over a date range and ships the counts to the given destination."""
DAY_GAP = 7 # The maximum number of simultaneous days to retrieve in a chunk.

def main():
    """
    Entry point and command line parser.
    """
    parser = ArgumentParser(description=PROGRAM_DESC, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-s", "--startdate", help="start date (format: YYYY-MM-DD), or yesterday if not specified")
    parser.add_argument("-e", "--enddate", help="end date, inclusive (format: YYYY-MM-DD), or startdate if not specified")
    parser.add_argument("-u", "--user", required=True, help="username for remote copying host")
    parser.add_argument("-H", "--hostname", required=True, help="remote hostname")
    parser.add_argument("-d", "--directory", required=True, help="target directory for remote copying host")
    args = parser.parse_args()

    if args.startdate:
        startDate = datetime.datetime.strptime(args.startdate, "%Y-%m-%d")
    else:
        startDate = datetime.datetime.now()
        startDate = startDate.replace(hour=0, minute=0, second=0, microsecond=0)
        startDate -= datetime.timedelta(days=1)
    if args.enddate:
        endDate = datetime.datetime.strptime(args.enddate, "%Y-%m-%d")
    else:
        endDate = startDate
    endDate += datetime.timedelta(days=1)

    user = args.user
    directory = args.directory
    hostname = args.hostname
    
    process(startDate, endDate, user, hostname, directory)
    return 0

def process(startDate, endDate, user, hostname, directory):
    """
    This is where the processing happens.
    """
    tempDir = None
    curDate = endDate
    while endDate > startDate:        
        try:        
            curDate = endDate - datetime.timedelta(days=DAY_GAP)
            if curDate < startDate:
                curDate = startDate
            
            # Create temporary directory:
            tempDir = tempfile.mkdtemp()
            
            # Output files to the temporary directory:
            #print("gs_getcounts.process(%s, %s, %s)" % (str(curDate), str(endDate), tempDir))
            gs_getcounts.process(curDate, endDate, tempDir)
            
            # Ship the files off to the remote:
            #print('scp "%s/"* "%s@%s:%s"' % (tempDir, user, hostname, directory))
            os.system('scp "%s/"* "%s@%s:%s"' % (tempDir, user, hostname, directory))
            
            endDate -= datetime.timedelta(days=DAY_GAP)
        finally:
            if tempDir:
                shutil.rmtree(tempDir)
            tempDir = None


if __name__ == "__main__":
    sys.exit(main())