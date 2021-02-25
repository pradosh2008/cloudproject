import os
import shutil

import pandas as pd
from pathlib import Path
from utils import util
from scripts.config import col1, unique_key1, drop1
from scripts.source_db import get_terra_data
from scripts.target_db import get_aws_data

# getting the parent folder directory path
fileDir = Path(__file__).parent.parent

# read excel file to pass the variable values to constants
df = pd.read_excel(os.path.join(fileDir, "resources\\variables.xlsx"), sheet_name='Sheet1')

for i in df.index:
    # constant variables set up
    columns = df['db_columns'][i].replace('"', '').strip().split(',')
    columns = [cols.strip() for cols in columns]

    unique_keys = df['unique_keys'][i].replace('"', '').strip().split(',')
    unique_keys = [key.strip() for key in unique_keys]

    drop_columns = df['drop_columns'][i].replace('"', '').strip().split(',')
    drop_columns = [cols.strip() for cols in drop_columns]

    # parse source configuration
    tera_ini_path = os.path.join(fileDir, "resources\\teradata_sql.ini")

    # parse target configuration
    aws_ini_path = os.path.join(fileDir, "resources\\aws_sql.ini")

    # read source data
    sdf = get_terra_data(df['query'][i], tera_ini_path)

    # read target data
    tdf = get_aws_data(df['query'][i], aws_ini_path)

    # save source data to csv file
    s_file_name = util.create_dir('input_files\\source\\', fileDir, 'input_{}.csv')
    sdf.to_csv(s_file_name, sep=',', index=None)

    # save target data to csv file
    t_file_name = util.create_dir('input_files\\target\\', fileDir, 'input_{}.csv')
    tdf.to_csv(t_file_name, sep=',', index=None)

    # create output directory is not exists already
    dir_path = 'output_files\\CAA\\'  # update output directory path name here

    # reading the csv files
    converters = {col: str for col in range(len(columns))}
    source_data = util.read_data_faster(s_file_name, converters, columns)
    target_data = util.read_data_faster(t_file_name, converters, columns)

    # drop data columns that are not required for inserting
    drop_s_df = source_data.drop(drop_columns, axis=1)
    drop_t_df = target_data.drop(drop_columns, axis=1)

    # sorting the values by ascending
    drop_s_df.sort_values(by=unique_keys)
    drop_t_df.sort_values(by=unique_keys)

    # inserting the missing columns with only unique key value into the target data frame
    s_df1 = drop_s_df[~(drop_s_df[unique_keys].isin(drop_t_df[unique_keys]))]
    new_s_df = pd.DataFrame(s_df1, columns=columns, dtype='str')
    target_data = pd.merge(new_s_df, target_data, how='outer').fillna('')

    # inserting the missing columns with only unique key value into the source data frame
    t_df1 = drop_t_df[~(drop_t_df[unique_keys].isin(drop_s_df[unique_keys]))]
    new_t_df = pd.DataFrame(t_df1, columns=columns, dtype='str')
    source_data = pd.merge(new_t_df, source_data, how='outer').fillna('')

    # display
    print('lines count of target after inserting the missing values for source filter:{}'
          .format(len(target_data)))
    print('lines count of source after inserting the missing values for target filter:{}'
          .format(len(source_data)))

    # condition to check if still the records are mis-matched, if still mis-matched we need to exist compare
    if len(target_data) != len(source_data):
        print('error occurred while updating the missing records to target file , '
              'still source lines count: {} while target lines count was :{}'.format(len(source_data), len(target_data)))
        exit()

    # sorting the values by ascending
    t_sort = target_data.sort_values(by=unique_keys)
    s_sort = source_data.sort_values(by=unique_keys)

    # creating a data list for data frame data so that we can compare row and find the mis-matching columns
    source, target = util.converting_df_to_iterable_list(s_sort, t_sort, ",")

    # creating the output directory and file name
    output_file_name = util.create_dir(dir_path, fileDir, 'difference_{}_caa.txt')

    # compare the two list outputs and out the differences in column values to text output file
    util.compare_line_by_line(output_file_name, source, target, ",")

    # clean up input files  directory
    shutil.rmtree(os.path.join(fileDir, 'input_files\\source\\'))
    shutil.rmtree(os.path.join(fileDir, 'input_files\\target\\'))
