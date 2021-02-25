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

