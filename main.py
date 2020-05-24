import os
from google.cloud import bigquery

def csv_loader(data, context):
        client = bigquery.Client()
        dataset_id = os.environ['DATASET']
        dataset_ref = client.dataset(dataset_id)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.schema = [
                bigquery.SchemaField('source', 'STRING'),
                bigquery.SchemaField('Commodity_Description', 'STRING'),
                bigquery.SchemaField('Country_Name', 'STRING'),
                bigquery.SchemaField('Market_Year', 'INTEGER'),
                bigquery.SchemaField('Attribute_Description', 'STRING'),
                bigquery.SchemaField('Unit_Description', 'STRING'),
                bigquery.SchemaField('Parameters', 'STRING'),
                bigquery.SchemaField('Value', 'FLOAT')
                ]
        job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.SourceFormat.CSV

        # get the URI for uploaded CSV in GCS from 'data'
        uri = 'gs://' + os.environ['BUCKET'] + '/' + data['name']

        # lets do this
        load_job = client.load_table_from_uri(
                uri,
                dataset_ref.table(os.environ['TABLE']),
                job_config=job_config)

        print('Starting job {}'.format(load_job.job_id))
        print('Function=csv_loader, Version=' + os.environ['VERSION'])
        print('File: {}'.format(data['name']))

        load_job.result()  # wait for table load to complete.
        print('Job finished.')

        destination_table = client.get_table(dataset_ref.table(os.environ['TABLE']))
        print('Loaded {} rows.'.format(destination_table.num_rows))
