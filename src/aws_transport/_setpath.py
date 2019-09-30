"""
Update current directory to use other resources provided by coa_dev project, which is effectively ".."
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
