"""
Created by:     Dr. Christos Hadjinikolis, Sr. Data Scientist | Data Reply UK
Date:           04/08/2017
Description:    This module is used for loading environment variables and directory paths.
"""
import os

import dotenv

# Configure path variables
PROJECT_DIR = os.path.join(os.path.dirname(__file__))
DATA = os.path.join(PROJECT_DIR, 'data')
EXTERNAL = os.path.join(DATA, 'external')
INTERIM = os.path.join(DATA, 'interim')
PROCESSED = os.path.join(DATA, 'processed')
RAW = os.path.join(DATA, 'raw')

# Load environment variables
DOTENV_PATH = os.path.join(PROJECT_DIR, '.env')
dotenv.load_dotenv(DOTENV_PATH)
