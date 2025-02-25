from api_request_funtions import ApiGet
from bq_transfers import BqDataTransfers

thrivecart_get = ApiGet(http='thrivecart.com', api_key='TZ5TJYBR-FDB85IBI-0RFTB00N-VQ7ZFY2S')
thrivecart_save = BqDataTransfers(gcp_project_id= 'arboreal-cat-451816-n0', bq_data_set = 'thrive_cart')
bq_client = thrivecart_save.get_bq_client("C:/Users/shami/iCloudDrive/Personal Projects/vexis/vexis_bq_writter.json")

def fetch_and_save(end_piont, destination_table):
    x = thrivecart_get.fetch_data(end_point=end_piont)
    df = thrivecart_get.process_reponse_df(x)
    df = thrivecart_get.fix_data_types(df)
    thrivecart_save.start_transfer(bq_client=bq_client, df = df, destination_table = destination_table)

# products
fetch_and_save(end_piont='/api/external/products', destination_table='products')
# bump offers
fetch_and_save(end_piont='/api/external/bumps', destination_table='bumps')
# upsells
fetch_and_save(end_piont='/api/external/upsells', destination_table='upsells')
# downsells
fetch_and_save(end_piont='/api/external/downsells', destination_table='downsells')

# based on ids