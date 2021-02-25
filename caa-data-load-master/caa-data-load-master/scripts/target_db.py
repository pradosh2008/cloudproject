from pyathena import connect
import pandas as pd
import configparser


def get_aws_data(query, ini_file_path):
    """
    function to extract the data from AWS athena database based on user provided sql
    :param query:
    :param ini_file_path:
    :return:
    """
    parser = configparser.ConfigParser()
    parser.read(ini_file_path)
    chunk_list = []
    try:
        conn = connect(aws_access_key_id=parser['AWS-DEV']['key_id'],   # 'YOUR_ACCESS_KEY_ID'
                       aws_secret_access_key=parser['AWS-DEV']['key'],  # 'YOUR_SECRET_ACCESS_KEY'
                       aws_session_token=parser['AWS-DEV']['token'],  # YOUR_SESSION_TOKEN
                       s3_staging_dir=parser['AWS-DEV']['s3'],  # s3 bucket path
                       region_name=parser['AWS-DEV']['region'])  # region from which this is accessed
        for chunk in pd.read_sql(parser['SQL-QUERIES'][query], conn, chunksize=70000):
            # append the chunk to list
            chunk_list.append(chunk)
        if len(chunk_list) > 0:
            # concat the list into data frame
            df_concat = pd.concat(chunk_list).convert_dtypes(convert_string=True)
            # print(df_concat)
            return df_concat
        else:
            return None
    except ConnectionError as e:
        print(e)


# get_aws_data('query1', r'C:\Users\Psibbal\PycharmProjects\caa-data-load\resources\aws_sql.ini')
