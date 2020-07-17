"""
cmdline.py: Common command line processing that creates an AppConfigObj

Kenneth Perrine
Center for Transportation Research, The University of Texas at Austin
"""
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from collections import namedtuple

CmdLineConfig = namedtuple("CmdLineConfig", "appName appDescr customArgs")
"""
appName: The name of the application (short string)
appDescr: Description of the application (longer string)
customArgs: Dictionary of add_argument parameter name or tuple of flags -> Dictionary of add_argument parameters
   (e.g. action -> String, default -> Boolean, etc.). Example {("-p", "--param"): {"help": "my parameter"}}
"""

def processArgs(cmdLineConfig):
    """
    Builds up the command line processor with standard parameters and also custom parameters that are passed in.
    """
    parser = ArgumentParser(prog=cmdLineConfig.appName,
                            description=cmdLineConfig.appDescr,
                            formatter_class=RawDescriptionHelpFormatter)
    # Tier-1 parameters:
    parser.add_argument("-r", "--last_run_date", help="last run date, in YYYY-MM-DD format with optional time zone offset")
    parser.add_argument("-s", "--start_date", help="start date; process no more than this number of months old, or provide YYYY-MM-DD for absolute date")
    parser.add_argument("-e", "--end_date", help="end date; process no later than this date, in YYYY-MM-DD format")
    parser.add_argument("-M", "--nomissing", action="store_true", default=False, help="don't check for missing entries after the earliest processing date")
    
    # Custom parameters:
    if cmdLineConfig.customArgs:
        for customArg in cmdLineConfig.customArgs:
            param = (customArg,) if not isinstance(customArg, list) and not isinstance(customArg, tuple) else customArg
            parser.add_argument(*param, **cmdLineConfig.customArgs[customArg]) 
    
    # Tier-2 parameters:
    parser.add_argument("-f", "--name_filter", default=".*", help="filter processing on units whose names match the given regexp")
    parser.add_argument("-o", "--output_filepath", help="specify a path to output files to a specific directory")
    parser.add_argument("-0", "--simulate", help="simulates the writing of files to the filestore and catalog")
    parser.add_argument("-L", "--logfile", help="enables logfile output to the given path")
    parser.add_argument("--log_autoname", help="automatically create the log name from app parameters")
    parser.add_argument("--debugmode", action="store_true", help="sets the code to run in debug mode, which usually causes access to non-production storage")
    
    # TODO: Consider parameters for writing out files?
    args = parser.parse_args()
    return args
