import teradatasql
import pandas as pd
import configparser


def get_terra_data(query, ini_file_path):
    """
    function to get extract the data from teradata using user provided sql
    :param query:
    :param ini_file_path:
    :return:
    """
    parser = configparser.ConfigParser()
    parser.read(ini_file_path)
    chunk_list = []
    try:
        with teradatasql.connect(host=parser['TERA-QA']['host'], user=parser['TERA-QA']['user'],
                                 password=parser['TERA-QA']['pw']) as connect:
            for chunk in pd.read_sql(parser['SQL-QUERIES'][query], connect, chunksize=70000):
                # append the chunk to list
                chunk_list.append(chunk)
            if len(chunk_list) > 0:
                # concat the list into data frame
                df_concat = pd.concat(chunk_list)
                # print(df_concat)
                return df_concat
            else:
                return None
    except ConnectionError as e:
        print(e)


# get_terra_data('query1', r'C:\Users\Psibbal\PycharmProjects\caa-data-load\resources\teradata_sql.ini')
