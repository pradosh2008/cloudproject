import pandas as pd
import numpy as np
import warnings
import datetime
import os
# to ignore the warning displayed for insert_row method
warnings.filterwarnings("ignore")


def create_dir(dir_path, file_dir, file_name):
    """
    function to create recurring directories
    :param dir_path:
    :param file_dir:
    :param file_name:
    :return:
    """
    # to get the name of the file based on file type we are working
    fn_name = dir_path.split('\\')[-2]
    output_dir = os.path.join(file_dir, dir_path + datetime.datetime.now().strftime('%m-%d-%Y_%H-%M-%S'))
    # update the output file name here
    output_file_name = os.path.join(output_dir, file_name.format(fn_name).lower())
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    return output_file_name


def converting_df_to_iterable_list(source_df, target_df, delimit_string):
    """
    function to convert a data frame to iterable list
    :param source_df:
    :param target_df:
    :param delimit_string:
    :return:
    """
    source_n = np.asarray(source_df)
    target_n = np.asarray(target_df)
    source = []
    target = []
    for item in source_n:
        y = item.tolist()
        source.append(delimit_string.join([str(elem) for elem in y]))
    for item in target_n:
        y = item.tolist()
        target.append(delimit_string.join([str(elem) for elem in y]))
    if len(source) > 0 and len(target) > 0:
        return source, target
    else:
        return None, None


def compare_line_by_line(output_file_name, list_source, list_target, delimit_string):
    """
    function to verify the two lists and output the differences from different columns
    :param output_file_name:
    :param list_source:
    :param list_target:
    :param delimit_string:
    :return:
    """
    # initializing a counter variable to keep the count of mis-match
    counter = 0

    # opening the output file in-memory to write the mismatches during source to target files validation
    with open(output_file_name, 'w') as out_file:
        # initializing a variable to keep the line count
        int_line = 0
        for s, t in zip(list_source, list_target):
            if s != t:
                counter = counter + 1
                text_list = []
                # split each column and verify if the source column data matches with target column data
                s_fields = s.split(delimit_string)
                t_fields = t.split(delimit_string)
                # initializing a variable to keep column count
                int_col = 0
                # iterating the column values to verify source and target values match
                for s_field, t_field in zip(s_fields, t_fields):
                    try:
                        # based on the source or target values that are converted by python to float, we will replace
                        # with null
                        if str(s_field).endswith('nan'):
                            s_field = str(s_field).replace('nan', '', 1)
                        if str(t_field).endswith('nan'):
                            t_field = str(t_field).replace('nan', '', 1)
                        if s_field != t_field:
                            text_list.append('Line :# {} COLUMN :# {} Source DATA: {} Target DATA: {} \n'.format(
                                int_line, int_col, s_field, t_field))
                        # incrementing the column value
                        int_col = int_col + 1
                    except (RuntimeError, TypeError, NameError) as e:
                        print('Exception occurred while comparing %s vs %s'.format(s_field, t_field))
                if len(text_list) > 0:
                    # writing source and target line item if there are any mismatched
                    out_file.write('Line# %r Source Data %r; Target Data %r \n' % (int_line, s.replace('\n', ''),
                                                                                  t.replace('\n', '')))
                    # looping all the lines to write into output file.
                    for line in text_list:
                        out_file.write(line)
                    out_file.write("\n")
            # incrementing the row value """
            int_line = int_line + 1
    # printing the no. of lines mis-match
    print("No. of Mismatch Records: {}".format(counter))


def read_data_faster(file_name, converters, columns):
    """
    function to read the large csv files faster, so that we avoid getting the low memory issues while reading the csv
    file.
    Increase the chuck size based on system memory and file size, else script will error with low memory
    :param file_name:
    :param converters: data type to be conversion
    :param columns: columns name to add as header to the file
    :return: data frame else none
    """
    chunk_list = []  # append each chunk df here
    # Each chunk is in df format
    for chunk in pd.read_csv(file_name, chunksize=70000, low_memory=False, converters=converters,
                             header=None, names=columns, sep=",", encoding='unicode_escape', skiprows=0):
        # append the chunk to list
        chunk_list.append(chunk)
    if len(chunk_list) > 0:
        # concat the list into data frame
        df_concat = pd.concat(chunk_list)
        df_concat = df_concat.astype('str')
        return df_concat
    else:
        return None
