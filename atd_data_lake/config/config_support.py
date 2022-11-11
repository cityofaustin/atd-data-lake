"""
Suport definitions for config package.

@author Kenneth Perrine
"""
from typing import NamedTuple

class DataSourceConfig(NamedTuple):
    code: str # A shorthand code name for the data source for directory naming
    name: str # A longer name that will appear in logs
