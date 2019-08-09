"""
plitmongo

A framework for PLIT mongodb scripts.
Created by Mickey Guo in Fall 2019.

:liscence: MIT
"""
__version__ = "0.0.1"

# Imports
import pandas as pd
from pymongo import MongoClient
import sys
import time
import os
from tqdm import tqdm