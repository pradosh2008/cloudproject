from google.cloud import bigquery
from google.cloud import storage

def import_to_big_query(data, context, dataset='edl_landing', table='ann_acc_df_prod_hier', verbose=True):
    def vprint(s):
        if verbose:
            print(s)

    vprint('Event ID: {}'.format(context.event_id))
    vprint('Event type: {}'.format(context.event_type))
    vprint('Importing required modules.')

    from google.cloud import bigquery

    vprint('This is the data: {}'.format(data))

    input_bucket_name = data['bucket']
    source_file = data['name']
    uri = 'gs://{}/{}'.format(input_bucket_name, source_file)

    vprint('Getting the data from bucket "{}"'.format(
        uri
    ))
    
    if str(source_file).lower().endswith('.txt') or \
            str(source_file).lower().endswith('.avro'):

        client = bigquery.Client()
        dataset_ref = client.dataset(dataset)

        job_config = bigquery.LoadJobConfig()
        #job_config.autodetect = True
        job_config.source_format = 'CSV'
        job_config.field_delimiter = '|'
        job_config.skip_leading_rows = 1
        job_config.quote_character = ""
        job_config.schema = [
			bigquery.SchemaField('SUBCLASS_ID', 'INTEGER', mode='NULLABLE'),
			bigquery.SchemaField('SUBCLASS_NAME', 'INTEGER', mode='NULLABLE'),
			bigquery.SchemaField('CLASS_ID', 'STRING', mode='NULLABLE'),
			bigquery.SchemaField('CLASS_NAME', 'STRING', mode='NULLABLE'),
			bigquery.SchemaField('DEPT_ID', 'INTEGER', mode='NULLABLE'),
			bigquery.SchemaField('DEPT_NAME', 'STRING', mode='NULLABLE'),
			bigquery.SchemaField('GROUP_ID', 'INTEGER', mode='NULLABLE'),
			bigquery.SchemaField('GROUP_NAME', 'INTEGER', mode='NULLABLE'),
			bigquery.SchemaField('DIV_ID', 'STRING', mode='NULLABLE'),
			bigquery.SchemaField('DIV_NAME', 'STRING', mode='NULLABLE'),
			bigquery.SchemaField('BRAND_ID', 'STRING', mode='NULLABLE'),
			bigquery.SchemaField('BRAND_NAME', 'STRING', mode='NULLABLE'),
			bigquery.SchemaField('STYLE_ID', 'INTEGER', mode='NULLABLE'),
			bigquery.SchemaField('STYLE_DESC', 'STRING', mode='NULLABLE'),
			bigquery.SchemaField('COLOR_ID', 'INTEGER', mode='NULLABLE'),
			bigquery.SchemaField('COLOR_DESC', 'STRING', mode='NULLABLE'),
			bigquery.SchemaField('STORE_SET_1', 'STRING', mode='NULLABLE'),
			bigquery.SchemaField('STORE_SET_2', 'STRING', mode='NULLABLE'),
			bigquery.SchemaField('ECOMM_EXCLUSIVE', 'BOOLEAN', mode='NULLABLE'),
			bigquery.SchemaField('TEST_IND', 'STRING', mode='NULLABLE'),
			bigquery.SchemaField('FASHION_PYRAMID', 'STRING', mode='NULLABLE')
    	]
        job_config.schema_update_options = [
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]

        if str(source_file).lower().endswith('.txt'):
            job_config.source_format = bigquery.SourceFormat.CSV
        else:
            job_config.source_format = bigquery.SourceFormat.AVRO

        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        load_job = client.load_table_from_uri(
            uri,
            dataset_ref.table(table),
            job_config=job_config)

        vprint('Starting job {}'.format(load_job.job_id))

        load_job.result()
        vprint('Job finished.')
		
        destination_table = client.get_table(dataset_ref.table(table))
        vprint('Loaded {} rows.'.format(destination_table.num_rows))
        vprint('table Name {}'.format(destination_table))
        
        vprint('File imported successfully.')
    else:
        vprint('Not an importable file.')