"""
Update current directory to use other resources provided in this project, which is effectively "../.."
"""
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), ".."))
