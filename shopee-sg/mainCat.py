import categories
import subcategories
from google.cloud import bigquery 
from google.oauth2 import service_account
import json
import io
import logging

chrome_path = "./chromedriver.exe"
project = 'shopee-mr' #config
dataset = 'shopee_sg' #config
cattable_name = 'Category'
subcattable_name = 'SubCategory'
cattable_id = f'{project}.{dataset}.{cattable_name}' #config
subcattable_id = f'{project}.{dataset}.{subcattable_name}' #config
bq_creds = "shopee-mr-0ac570e2c1c3.json" #config
debug_log_path = 'log/maincat_debug.log'
info_log_path = 'log/maincat_info.log'
shopee_url = 'https://shopee.sg'

def init_log(debug_log_path, info_log_path):
    logging.basicConfig(filename=debug_log_path, level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    info_log = logging.FileHandler(info_log_path, mode='a')
    info_log.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    info_log.setFormatter(formatter)
    logging.getLogger('').addHandler(info_log)

    logging.info("Main Log Started")
    
#while True:           

init_log(debug_log_path, info_log_path)
logging.info('----- Getting Categories -----')
df = categories.main(chrome_path, project, dataset, cattable_name, cattable_id, bq_creds, shopee_url)
logging.info(df)

#logging_warning('Error with getting products')
logging.info('----- Getting Subcategories -----')
subcategories.main(chrome_path, df, project, dataset, subcattable_name, subcattable_id, bq_creds)

#logging_warning('Error with getting product details')

logging.shutdown()