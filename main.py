from api_request_funtions import ApiGet
from api_request_funtions import ApiGetRequest
from bq_transfers import BqDataTransfers
from  bq_transfers import pub_sub_message_publisher
import json
import io
import time

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
        df = df[df['statusString'] == 'live']
        ids = df[id_col].dropna().unique()
        return ids

def fetch_products_details(event, context):
    product_ids = fetch_and_save(end_point='/api/external/products', destination_table='products', id_col="product_id")
    try:
        fetch_and_save(end_point='/api/external/products/{}/pricing_options?affiliate_id=',
                       destination_table='product_price_details', use_ids=product_ids, write_options ='append')
        update_table = """create or replace table `arboreal-cat-451816-n0.thrive_cart.product_price_details`  as 
                           SELECT distinct * FROM `arboreal-cat-451816-n0.thrive_cart.product_price_details` """
        query_job = bq_client.query(update_table)
        query_job.result()
    except:
        pass
    pub_sub_message_publisher(project_id=project_id, topic='thrive_cart_products2_trigger',
                              message='Continue products')


def fetch_products_info(event, context):
    product_ids = fetch_and_save(end_point='/api/external/products', destination_table='products', id_col="product_id")
    fetch_and_save(end_point='/api/external/products/{}', destination_table='product_info', use_ids=product_ids,
                   write_options='append')
    update_table = """create or replace table `arboreal-cat-451816-n0.thrive_cart.product_info`  as
                           SELECT distinct * FROM `arboreal-cat-451816-n0.thrive_cart.product_info` """
    query_job = bq_client.query(update_table)
    query_job.result()

def fetch_bumps(event, context):
    bump_ids = fetch_and_save(end_point='/api/external/bumps', destination_table='bumps', id_col='bump_id')
    try:
        fetch_and_save(end_point='/api/external/bumps/{}/pricing_options',
                       destination_table='bump_price_details', use_ids=bump_ids)
        update_table = """create or replace table `arboreal-cat-451816-n0.thrive_cart.bump_price_details`  as
                                   SELECT distinct * FROM `arboreal-cat-451816-n0.thrive_cart.bump_price_details` """
        query_job = bq_client.query(update_table)
        query_job.result()

        fetch_and_save(end_point='/api/external/bumps/{}', destination_table='bumps_info', use_ids=bump_ids)

        update_table = """create or replace table `arboreal-cat-451816-n0.thrive_cart.bumps_info`  as
                                   SELECT distinct * FROM `arboreal-cat-451816-n0.thrive_cart.bumps_info` """
        query_job = bq_client.query(update_table)
        query_job.result()
    except:
        pass
    pub_sub_message_publisher(project_id=project_id, topic ='thrive_cart_downsells_trigger',
                              message = 'Start downsells')

def fetch_downsells(event, context):
    downsell_id = fetch_and_save(end_point='/api/external/downsells', destination_table='downsells', id_col='downsell_id')
    try:
        fetch_and_save(end_point='/api/external/downsells/{}/pricing_options',
                       destination_table='downsells_price_details', use_ids=downsell_id)
        update_table = """create or replace table `arboreal-cat-451816-n0.thrive_cart.downsells_price_details`  as
                            SELECT distinct * FROM `arboreal-cat-451816-n0.thrive_cart.downsells_price_details` """
        query_job = bq_client.query(update_table)
        query_job.result()

        fetch_and_save(end_point='/api/external/downsells/{}', destination_table='downsells_info', use_ids=downsell_id)
        update_table = """create or replace table `arboreal-cat-451816-n0.thrive_cart.downsells_info`  as
                            SELECT distinct * FROM `arboreal-cat-451816-n0.thrive_cart.downsells_info` """
        query_job = bq_client.query(update_table)
        query_job.result()
    except:
        pass
    pub_sub_message_publisher(project_id=project_id, topic='thrive_cart_upsells_trigger',
                              message='Start upsells')

def fetch_upsells(event, context):
    upsell_ids = fetch_and_save(end_point='/api/external/upsells', destination_table='upsells', id_col='upsell_id')
    try:
        fetch_and_save(end_point='/api/external/upsells/{}/pricing_options',
                       destination_table='upsells_price_details', use_ids=upsell_ids)
        update_table = """create or replace table `arboreal-cat-451816-n0.thrive_cart.upsells_price_details`  as
                            SELECT distinct * FROM `arboreal-cat-451816-n0.thrive_cart.upsells_price_details` """
        query_job = bq_client.query(update_table)
        query_job.result()

        fetch_and_save(end_point='/api/external/upsells/{}', destination_table='upsells_info', use_ids=upsell_ids)
        update_table = """create or replace table `arboreal-cat-451816-n0.thrive_cart.upsells_info`  as
                            SELECT distinct * FROM `arboreal-cat-451816-n0.thrive_cart.upsells_info` """
        query_job = bq_client.query(update_table)
        query_job.result()
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
                              write_options='append', destination_table = 'webinars', id_col = 'webinar_id')
        fetch_and_save_webjam(platform='webinarjam', end_point='webinar', dict_key = 'webinar',
                              write_options='append', destination_table = 'webinar_details', use_ids=webinar_ids)
        fetch_and_save_webjam(platform='webinarjam', end_point='registrants', dict_key = 'registrants',
                              write_options='append', destination_table = 'registrants', use_ids=webinar_ids, registrants=True)
    except:
        pass
    pub_sub_message_publisher(project_id=project_id, topic='webinar_jam_everwebinar_trigger', message='Start everwebinar')

def fetch_everwebinar(event, context):
    webinar_ids = fetch_and_save_webjam(platform='everwebinar', end_point='webinars', dict_key = 'webinars',
                          write_options='append', destination_table = 'webinars', id_col = 'webinar_id')
    fetch_and_save_webjam(platform='everwebinar', end_point='webinar', dict_key = 'webinar',
                          write_options='append', destination_table = 'webinar_details', use_ids=webinar_ids)
    fetch_and_save_webjam(platform='everwebinar', end_point='registrants', dict_key = 'registrants',
                          write_options='append', destination_table = 'registrants', use_ids=webinar_ids, registrants=True)
