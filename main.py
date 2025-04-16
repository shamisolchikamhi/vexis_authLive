from api_request_funtions import ApiGet
from api_request_funtions import ApiGetRequest
from bq_transfers import BqDataTransfers
from  bq_transfers import pub_sub_message_publisher
import json
import io
import time
from urllib.request import Request, urlopen
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

project_id = 'arboreal-cat-451816-n0'
thrivecart_get = ApiGet(http='thrivecart.com', api_key='TZ5TJYBR-FDB85IBI-0RFTB00N-VQ7ZFY2S')
thrivecart_save = BqDataTransfers(gcp_project_id= project_id, bq_data_set = 'thrive_cart')
bq_client = thrivecart_save.get_bq_client("/Users/shami/Library/Mobile Documents/com~apple~CloudDocs/Personal Projects/vexis/vexis_bq_writter.json")

def fetch_and_save(end_point, destination_table, write_options ='overwrite', use_ids=[], id_col = None, schema = None):
    if len(use_ids) > 0:
        x = thrivecart_get.fetch_data_id(end_point=end_point, ids=use_ids)
    else:
        x = thrivecart_get.fetch_data(end_point=end_point)

    df = thrivecart_get.process_reponse_df(x)
    df = thrivecart_get.fix_data_types(df)
    thrivecart_save.start_transfer_df(bq_client=bq_client, df = df,
                                      destination_table = destination_table, write_options=write_options, schema=schema)

    if id_col is not None:
        ids = df[id_col].dropna().unique()
        return ids

def fetch_products(event, context):
    product_ids = fetch_and_save(end_point='/api/external/products', destination_table='products', id_col="product_id")
    fetch_and_save(end_point='/api/external/products/{}/pricing_options?affiliate_id=',
                   destination_table='product_price_details', use_ids=product_ids)
    fetch_and_save(end_point='/api/external/products/{}', destination_table='product_info', use_ids=product_ids)


def fetch_bumps(event, context):
    bump_ids = fetch_and_save(end_point='/api/external/bumps', destination_table='bumps', id_col='bump_id')
    try:
        fetch_and_save(end_point='/api/external/bumps/{}/pricing_options',
                       destination_table='bump_price_details', use_ids=bump_ids)
        fetch_and_save(end_point='/api/external/bumps/{}', destination_table='bumps_info', use_ids=bump_ids)
    except:
        pass
    pub_sub_message_publisher(project_id=project_id, topic ='thrive_cart_downsells_trigger',
                              message = 'Start downsells')

def fetch_downsells(event, context):
    downsell_id = fetch_and_save(end_point='/api/external/downsells', destination_table='downsells', id_col='downsell_id')
    try:
        fetch_and_save(end_point='/api/external/downsells/{}/pricing_options',
                       destination_table='downsells_price_details', use_ids=downsell_id)
        fetch_and_save(end_point='/api/external/downsells/{}', destination_table='downsells_info', use_ids=downsell_id)
    except:
        pass
    pub_sub_message_publisher(project_id=project_id, topic='thrive_cart_upsells_trigger',
                              message='Start upsells')

def fetch_upsells(event, context):
    upsell_ids = fetch_and_save(end_point='/api/external/upsells', destination_table='upsells', id_col='upsell_id')
    try:
        fetch_and_save(end_point='/api/external/upsells/{}/pricing_options',
                       destination_table='upsells_price_details', use_ids=upsell_ids)
        fetch_and_save(end_point='/api/external/upsells/{}', destination_table='upsells_info', use_ids=upsell_ids)
    except:
        pass
    pub_sub_message_publisher(project_id=project_id, topic='thrive_cart_affiliates_trigger',
                              message='Start affiliates')


def fetch_affiliates(event, context):
    try:
        # Fetch the total count of affiliates from the API
        x = thrivecart_get.fetch_data(
            end_point=f'https://thrivecart.com/api/external/affiliates?product_id&query=&page=1&perPage=5')
        total_records_api = x['meta']['total']
        total_records_bq = bq_client.get_table('thrive_cart.affiliates').num_rows

        # Calculate the difference and determine the number of pages to fetch
        records_to_fetch = max(0, total_records_api - total_records_bq)
        pages = list(range(1, (records_to_fetch // 100) + 2)) if records_to_fetch > 0 else []
        pages = reversed(pages)
        for page in pages:
            x = thrivecart_get.fetch_data(
                end_point=f'https://thrivecart.com/api/external/affiliates?product_id&query=&page={page}&perPage=25'
            )
            transactions = x.get("affiliates", [])

            if transactions:
                # Convert to NDJSON format
                ndjson_lines = "\n".join(json.dumps(transaction) for transaction in transactions)
                ndjson_buffer = io.StringIO(ndjson_lines)

                # Upload to BigQuery, ensuring no duplicates based on transaction_id
                thrivecart_save.start_transfer_json(
                    bq_client=bq_client,
                    file=ndjson_buffer,
                    destination_table='affiliates',
                    write_options='append'
                )

            time.sleep(1)
    except:
        pass

#     remove duplicates if any
    query = """create or replace table
            `arboreal-cat-451816-n0.thrive_cart.affiliates`
            as
            SELECT distinct * FROM `arboreal-cat-451816-n0.thrive_cart.affiliates`;
            """
    query_job = bq_client.query(query)
    query_job.result()

    pub_sub_message_publisher(project_id=project_id, topic='thrive_cart_products_trigger',
                              message='Start products')


def fetch_transactions(event, context):
    # Fetch the total count of transactions from the API
    x = thrivecart_get.fetch_data(end_point=f'/api/external/transactions?page=0&perPage=0&query=&transactionType=any')
    total_records_api = x['meta']['total']
    total_records_bq = bq_client.get_table('thrive_cart.transactions').num_rows

    # Calculate the difference and determine the number of pages to fetch
    records_to_fetch = max(0, total_records_api - total_records_bq)
    pages = list(range(1, (records_to_fetch // 100) + 2)) if records_to_fetch > 0 else []
    pages = reversed(pages)

    BIGQUERY_SCHEMA = [
        {"name": "quantity", "type": "INTEGER"},
        {"name": "item_type", "type": "STRING"},
        {"name": "processor", "type": "STRING"},
        {"name": "currency", "type": "STRING"},
        {"name": "coupon", "type": "STRING"},
        {"name": "invoice_id", "type": "INTEGER"},
        {"name": "order_id", "type": "INTEGER"},
        {"name": "internal_subscription_id", "type": "INTEGER"},
        {"name": "item_pricing_option_id", "type": "INTEGER"},
        {"name": "time", "type": "TIMESTAMP"},
        {"name": "item_id", "type": "INTEGER"},
        {
            "name": "customer",
            "type": "RECORD",
            "fields": [
                {"name": "email", "type": "STRING"},
                {"name": "name", "type": "STRING"}
            ]
        },
        {"name": "amount", "type": "INTEGER"},
        {"name": "transaction_info", "type": "STRING"},
        {"name": "subscription_id", "type": "STRING"},
        {"name": "item_name", "type": "STRING"},
        {"name": "transaction_type", "type": "STRING"},
        {"name": "reference", "type": "STRING"},
        {"name": "item_pricing_option_name", "type": "STRING"},
        {"name": "date", "type": "DATE"},
        {"name": "campaign_id", "type": "STRING"},
        {"name": "transaction_id", "type": "STRING"},
        {"name": "event_id", "type": "INTEGER"},
        {"name": "base_product", "type": "INTEGER"},
        {"name": "timestamp", "type": "INTEGER"}
    ]

    # Fetch new transactions
    for page in pages:
        x = thrivecart_get.fetch_data(
            end_point=f'/api/external/transactions?page={page}&perPage=100&query=&transactionType=any'
        )
        transactions = x.get("transactions", [])

        if transactions:
            # Convert to NDJSON format
            ndjson_lines = "\n".join(json.dumps(transaction) for transaction in transactions)
            ndjson_buffer = io.StringIO(ndjson_lines)

            # Upload to BigQuery, ensuring no duplicates based on transaction_id
            thrivecart_save.start_transfer_json(
                bq_client=bq_client,
                file=ndjson_buffer,
                destination_table='transactions',
                write_options='append',
                schema=BIGQUERY_SCHEMA
            )

        time.sleep(1)

#     remove duplicates if any
    query = """create or replace table
            `arboreal-cat-451816-n0.thrive_cart.transactions_copy`
            as
            SELECT distinct * FROM `arboreal-cat-451816-n0.thrive_cart.transactions` WHERE date = current_date() ;
            delete from `arboreal-cat-451816-n0.thrive_cart.transactions` WHERE date = current_date();
            insert into `arboreal-cat-451816-n0.thrive_cart.transactions`
            select * from `arboreal-cat-451816-n0.thrive_cart.transactions_copy`;
            drop table `arboreal-cat-451816-n0.thrive_cart.transactions_copy`;
            """
    query_job = bq_client.query(query)
    query_job.result()


# WEBJAM
def fetch_and_save_webjam(platform, end_point, dict_key, write_options, destination_table,
                          schema = None, use_ids=[], id_col = None, registrants = False):
    webjam = ApiGetRequest(domain = 'api.webinarjam.com', platform=platform, api_key='8f4a91cb-906e-448b-b5cb-de0a1854d96c')
    webjam_save = BqDataTransfers(gcp_project_id= 'arboreal-cat-451816-n0', bq_data_set = 'webinar_jam')
    bq_client = webjam_save.get_bq_client("/Users/shami/Library/Mobile Documents/com~apple~CloudDocs/Personal Projects/vexis/vexis_bq_writter.json")

    if len(use_ids) > 0:
        x = webjam.fetch_data_id(end_point=end_point, data_details_key= "webinar_id", ids=use_ids, registrants=registrants)
    else:
        x = webjam.fetch_data(end_point=end_point)
    df = webjam.process_reponse_df(x, dict_key=dict_key)
    if len(df) > 0:
        webjam_save.start_transfer_df(bq_client=bq_client, df = df,
                                              destination_table = f'{platform}_{destination_table}',
                                      write_options=write_options, schema=schema)
    if id_col is not None:
        ids = df[id_col].dropna().unique()
        return ids

    if registrants:
        query = f"""create or replace table
                `arboreal-cat-451816-n0.webinar_jam.{platform}_registrants_copy`
                as
                SELECT distinct * FROM `arboreal-cat-451816-n0.webinar_jam.{platform}_registrants` WHERE date(signup_date) >= current_date() ;
                delete from `arboreal-cat-451816-n0.webinar_jam.{platform}_registrants` WHERE date(signup_date) >= current_date();
                insert into `arboreal-cat-451816-n0.webinar_jam.{platform}_registrants`
                select * from `arboreal-cat-451816-n0.webinar_jam.{platform}_registrants_copy`;
                drop table `arboreal-cat-451816-n0.webinar_jam.{platform}_registrants_copy`;"""
        query_job = bq_client.query(query)
        query_job.result()


def fetch_webinarjam(event, context):
    try:
        webinar_ids = fetch_and_save_webjam(platform='webinarjam', end_point='webinars', dict_key = 'webinars',
                              write_options='overwrite', destination_table = 'webinars', id_col = 'webinar_id')
        fetch_and_save_webjam(platform='webinarjam', end_point='webinar', dict_key = 'webinar',
                              write_options='overwrite', destination_table = 'webinar_details', use_ids=webinar_ids)
        fetch_and_save_webjam(platform='webinarjam', end_point='registrants', dict_key = 'registrants',
                              write_options='append', destination_table = 'registrants', use_ids=webinar_ids, registrants=True)
    except:
        pass
    pub_sub_message_publisher(project_id=project_id, topic='webinar_jam_everwebinar_trigger', message='Start everwebinar')

def fetch_everwebinar(event, context):
    webinar_ids = fetch_and_save_webjam(platform='everwebinar', end_point='webinars', dict_key = 'webinars',
                          write_options='overwrite', destination_table = 'webinars', id_col = 'webinar_id')
    fetch_and_save_webjam(platform='everwebinar', end_point='webinar', dict_key = 'webinar',
                          write_options='overwrite', destination_table = 'webinar_details', use_ids=webinar_ids)
    fetch_and_save_webjam(platform='everwebinar', end_point='registrants', dict_key = 'registrants',
                          write_options='append', destination_table = 'registrants', use_ids=webinar_ids, registrants=True)

# hyros

headers = {
    'API-Key': 'API_c1c14fe7384d5050491b9d0c401184bd6facb348ccc8c59d2a53cdcec9c14332'
}
hyros_save = BqDataTransfers(gcp_project_id=project_id, bq_data_set='hyros')
bq_client = hyros_save.get_bq_client(
    "/Users/shami/Library/Mobile Documents/com~apple~CloudDocs/Personal Projects/vexis/vexis_bq_writter.json"
)

def _process_and_save_df(df: pd.DataFrame, table_id: str, date, page_id):
    """
    Aligns and uploads the DataFrame to BigQuery.
    Adds missing columns to table or DataFrame to ensure schema match.
    """
    print(f"🔄 Processing data for {table_id} | Date: {date} | Page: {page_id}")
    try:
        df['creationDate'] = pd.to_datetime(df['creationDate']).dt.tz_convert('UTC')
    except:
        df['creationDate'] = pd.to_datetime(df['creationDate'], unit='ms', utc=True)
    try:
        df.columns = df.columns.str.replace(r'\.', '_', regex=True)
        align_and_upload_to_bq(df, f"hyros.{table_id}", project_id=hyros_save.gcp_project_id)
    except Exception as e:
        print(f"Failed to process and upload data: {e}")


def align_and_upload_to_bq(df, table_id, project_id):
    # Flatten list-like columns (arrays) into strings to prevent pyarrow issues
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).any():
            print(f"⚠️ Flattening list column: {col}")
            df[col] = df[col].apply(lambda x: ','.join(map(str, x)) if isinstance(x, list) else x)

    # Get current table schema
    table = bq_client.get_table(table_id)
    bq_schema = table.schema

    # Create sets for comparison
    df_columns = set(df.columns)
    bq_columns = set(field.name for field in bq_schema)

    # Columns in df but not in BigQuery table (need to add to BQ table)
    columns_to_add_to_bq = df_columns - bq_columns

    for col in columns_to_add_to_bq:
        dtype = df[col].dropna().infer_objects().dtype
        if pd.api.types.is_string_dtype(dtype):
            field_type = "STRING"
        elif pd.api.types.is_bool_dtype(dtype):
            field_type = "BOOLEAN"
        elif pd.api.types.is_integer_dtype(dtype):
            field_type = "INT64"
        elif pd.api.types.is_float_dtype(dtype):
            field_type = "FLOAT64"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            field_type = "TIMESTAMP"
        else:
            field_type = "STRING"  # fallback for complex types

        query = f"""
        ALTER TABLE `{table_id}`
        ADD COLUMN `{col}` {field_type};
        """
        bq_client.query(query).result()
        print(f"➕ Added column `{col}` ({field_type}) to {table_id}")

    # Columns in BQ table but not in df (need to add as nulls)
    columns_to_add_to_df = bq_columns - df_columns
    for col in columns_to_add_to_df:
        dtype = next(field.field_type for field in bq_schema if field.name == col)
        if dtype == "STRING":
            df[col] = None
        elif dtype in ["INT64", "FLOAT64", "BOOLEAN"]:
            df[col] = np.nan
        elif dtype == "TIMESTAMP":
            df[col] = pd.NaT
        else:
            df[col] = None  # fallback

    # Reorder columns to match BQ schema
    df = df[[field.name for field in bq_schema]]

    # Upload to BigQuery
    job = bq_client.load_table_from_dataframe(df, table_id)
    job.result()  # Wait for completion
    print(f"✅ Uploaded {len(df)} rows to {table_id}")

def fetch_and_store_hyros_data(
    endpoint: str,
    destination_table: str,
    start_date: str = None,
    end_date: str = None,
    use_date_and_pagination: bool = True,
):
    resume_table = f"{destination_table}_resume_state"
    dataset_id = hyros_save.bq_data_set
    full_table_id = f"{hyros_save.gcp_project_id}.{dataset_id}.{resume_table}"

    def get_resume_state():
        try:
            query = f"SELECT page_id FROM `{full_table_id}` LIMIT 1"
            result = bq_client.query(query).result()
            row = next(result, None)
            if row:
                return int(row.page_id)
        except Exception as e:
            print(f"Couldn't read resume state from BQ: {e}")
        return 1  # default page_id

    def save_resume_state(page_id):
        df = pd.DataFrame([{"page_id": int(page_id)}])
        hyros_save.start_transfer_df(
            bq_client=bq_client,
            df=df,
            destination_table=resume_table,
            write_options='overwrite'
        )
        print(f"Saved resume state: page_id={page_id}")

    if use_date_and_pagination:
        if not start_date or not end_date:
            raise ValueError("Both 'start_date' and 'end_date' are required when using pagination.")

        # Convert to full-day datetime strings
        start_dt = datetime.fromisoformat(start_date).replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = datetime.fromisoformat(end_date).replace(hour=23, minute=59, second=59, microsecond=0)
        fromDate = start_dt.isoformat()
        toDate = end_dt.isoformat()

        page_id = get_resume_state()
        page_counter = 0
        collected_pages = []

        while True:
            url = f"https://api.hyros.com/v1/api/v1.0/{endpoint}?fromDate={fromDate}&toDate={toDate}&pageSize=250&pageId={page_id}"
            print(f"Request: {url}")
            request = Request(url, headers=headers)

            try:
                response_body = urlopen(request).read()
                json_str = response_body.decode('utf-8')
                data = json.loads(json_str)
                page_df = pd.json_normalize(data['result'])

                if page_df.empty:
                    print(f"No data for date range {start_date} to {end_date}, page {page_id}")
                    break

                collected_pages.append(page_df)
                page_counter += 1
                next_page = data.get('nextPageId')

                # Process & save every 10 pages
                if page_counter % 10 == 0:
                    combined_df = pd.concat(collected_pages, ignore_index=True)
                    _process_and_save_df(combined_df, destination_table, start_date, page_id)
                    collected_pages = []
                    save_resume_state(next_page)

                # If no more pages, process remaining data and save resume
                if not next_page:
                    if collected_pages:
                        combined_df = pd.concat(collected_pages, ignore_index=True)
                        _process_and_save_df(combined_df, destination_table, start_date, page_id)
                    save_resume_state(1)  # reset tracker
                    break

                page_id = next_page

            except Exception as e:
                print(f"Error on page {page_id}: {e}")
                return

    else:
        print(f"Fetching non-date endpoint: {endpoint}")
        url = f"https://api.hyros.com/v1/api/v1.0/{endpoint}"
        request = Request(url, headers=headers)

        try:
            response_body = urlopen(request).read()
            json_str = response_body.decode('utf-8')
            data = json.loads(json_str)
            df = pd.json_normalize(data['result'])

            if df.empty:
                print("No data returned.")
            else:
                _process_and_save_df(df, destination_table, "n/a", "n/a")
        except Exception as e:
            print(f"API call failed: {e}")
today = datetime.utcnow().date().isoformat() 
def hyros_sales(event, context):
    fetch_and_store_hyros_data(
        endpoint='sales',
        destination_table='sales',
        start_date='2024-01-01',
        end_date='2024-12-31'
    )

def hyros_leads(event, context):
    fetch_and_store_hyros_data(
        endpoint='leads',
        destination_table='leads',
        start_date='2024-01-01',
        end_date='2024-12-31'
    )

def hyros_ads(event, context):
    fetch_and_store_hyros_data(
        endpoint='ads',
        destination_table='ads',
        start_date='2025-01-01',
        end_date='2025-12-31'
    )
