"""
Created by Dr. Christos Hadjinikolis
Sr. Data Scientist | Data Reply UK

Description: This module is used for loading environment variables and directory paths.
"""
import os

import dotenv

project_dir = os.path.join(os.path.dirname(__file__), os.pardir)
dotenv_path = os.path.join(project_dir, '.env')
dotenv.load_dotenv(dotenv_path)
