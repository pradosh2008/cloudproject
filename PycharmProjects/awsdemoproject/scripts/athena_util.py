import os
import shutil

import pandas as pd
from pathlib import Path
from utils import util
#from scripts.config import col1, unique_key1, drop1
#from scripts.source_db import get_terra_data
from scripts.target_db import get_aws_data



# getting the parent folder directory path
fileDir = Path(__file__).parent.parent

# parse source configuration
#tera_ini_path = os.path.join(fileDir, "resources\\teradata_sql.ini")

# parse target configuration
aws_ini_path = os.path.join(fileDir, "resources\\aws_credentials.ini")

# read target data
tdf = get_aws_data('query1', aws_ini_path)

# save target data to csv file
t_file_name = util.create_dir('input_files\\target\\', fileDir, 'input_{}.csv')
tdf.to_csv(t_file_name, sep=',', index=None)