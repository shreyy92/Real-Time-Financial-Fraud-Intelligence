"""Pytest configuration and global environment fixtures for Member 2."""

import os
import sys

# Ensure PySpark workers on Windows always use the active Python virtual environment
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
