from google.cloud import bigquery
from google.oauth2 import service_account

class BqDataTransfers:
    def __init__(self, gcp_project_id, bq_data_set):
        self.gcp_project_id = gcp_project_id
        self.bq_data_set = bq_data_set

    def get_bq_client(self, service_account_json):
        credentials = service_account.Credentials.from_service_account_file(
            service_account_json
        )
        project_id = self.gcp_project_id
        bq_client = bigquery.Client(project_id, credentials)

        return bq_client


    def bq_table_id(self, destination_table):
        table_id = f"{self.gcp_project_id}.{self.bq_data_set}.{destination_table}"

        return table_id

    def start_transfer(self, bq_client ,df, destination_table, schema=None, write_options='overwrite',
                       clustering=None):

        if write_options == 'overwrite':
            write_options = bigquery.WriteDisposition.WRITE_TRUNCATE
        elif write_options == 'append':
            write_options = bigquery.WriteDisposition.WRITE_APPEND
        else:
            write_options = bigquery.WriteDisposition.WRITE_EMPTY

        job_configuration = bigquery.LoadJobConfig(
            schema=schema,
            autodetect=True,
            max_bad_records=1000,
            clustering_fields=clustering
        )

        job_configuration.write_disposition = write_options
        table_id = self.bq_table_id(destination_table=destination_table)
        bq_client.load_table_from_dataframe(df, table_id, job_config=job_configuration).result()
