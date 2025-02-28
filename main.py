from api_request_funtions import ApiGet
from bq_transfers import BqDataTransfers
import json
import io
import time

thrivecart_get = ApiGet(http='thrivecart.com', api_key='TZ5TJYBR-FDB85IBI-0RFTB00N-VQ7ZFY2S')
thrivecart_save = BqDataTransfers(gcp_project_id= 'arboreal-cat-451816-n0', bq_data_set = 'thrive_cart')
bq_client = thrivecart_save.get_bq_client("/Users/shami/Library/Mobile Documents/com~apple~CloudDocs/Personal Projects/vexis/vexis_bq_writter.json")

def fetch_and_save(end_piont, destination_table, write_options = 'overwrite', use_ids=[], id_col = None):
    if len(use_ids) > 0:
        x = thrivecart_get.fetch_data_id(end_point=end_piont, ids=use_ids)
    else:
        x = thrivecart_get.fetch_data(end_point=end_piont)

    df = thrivecart_get.process_reponse_df(x)
    df = thrivecart_get.fix_data_types(df)
    thrivecart_save.start_transfer_df(bq_client=bq_client, df = df,
                                      destination_table = destination_table, write_options=write_options)

    if id_col is not None:
        ids = df[id_col].dropna().unique()
        return ids

def fetch_products(event, context):
    product_ids = fetch_and_save(end_piont='/api/external/products', destination_table='products', id_col="product_id")
    fetch_and_save(end_piont='/api/external/products/{}', destination_table='product_info', use_ids=product_ids)
    fetch_and_save(end_piont='/api/external/products/{}/pricing_options?affiliate_id=',
                   destination_table='product_price_details', use_ids=product_ids)

def fetch_bumps(event, context):
    time.sleep(30)
    bump_ids = fetch_and_save(end_piont='/api/external/bumps', destination_table='bumps', id_col='bump_id')
    fetch_and_save(end_piont='/api/external/bumps/{}', destination_table='bumps_info', use_ids=bump_ids)
    fetch_and_save(end_piont='/api/external/bumps/{}/pricing_options',
                   destination_table='bump_price_details', use_ids=bump_ids)

def fetch_upsells(event, context):
    time.sleep(60)
    upsell_ids = fetch_and_save(end_piont='/api/external/upsells', destination_table='upsells', id_col='upsell_id')
    fetch_and_save(end_piont='/api/external/upsells/{}', destination_table='upsells_info', use_ids=upsell_ids)
    fetch_and_save(end_piont='/api/external/upsells/{}/pricing_options',
                   destination_table='upsells_price_details', use_ids=upsell_ids)

def fetch_downsells(event, context):
    time.sleep(90)
    downsell_id = fetch_and_save(end_piont='/api/external/downsells', destination_table='downsells', id_col='downsell_id')
    fetch_and_save(end_piont='/api/external/downsells/{}', destination_table='downsells_info', use_ids=downsell_id)
    fetch_and_save(end_piont='/api/external/downsells/{}/pricing_options',
                   destination_table='downsells_price_details', use_ids=downsell_id)

def fetch_transactions(event, context):
    x = thrivecart_get.fetch_data(end_point=f'/api/external/transactions?page=0&perPage=0&query=&transactionType=any')
    # pages = list(range(1, round(x['meta']['total'] / 100) + 1))
    pages = list(range(1, 5))  #limiting to 5 pages for now
    for page in pages:
        x = thrivecart_get.fetch_data(
            end_point=f'/api/external/transactions?page={page}&perPage=100&query=&transactionType=any')
        ndjson_lines = "\n".join(json.dumps(transaction) for transaction in x["transactions"])
        ndjson_buffer = io.StringIO(ndjson_lines)
        thrivecart_save.start_transfer_json(bq_client=bq_client, file=ndjson_buffer,
                                              destination_table = 'transactions', write_options='append')
        time.sleep(0.5)
