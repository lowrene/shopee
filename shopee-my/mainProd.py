import product
import productdetails
from google.cloud import bigquery 
from google.oauth2 import service_account
import json
import io
import logging

chrome_path = "./chromedriver.exe"
project = 'shopee-mr' #config
dataset = 'shopee_my' #config
prodtable_name = 'Product'
prodDtable_name = 'ProductDetails'
prodtable_id = f'{project}.{dataset}.{prodtable_name}' #config
prodDtable_id = f'{project}.{dataset}.{prodDtable_name}' #config
bq_creds = "shopee-mr-0ac570e2c1c3.json" #config
debug_log_path = 'log/mainprod_debug.log'
info_log_path = 'log/mainprod_info.log'


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
logging.info('----- Getting Products -----')
product.main(chrome_path, project, dataset, prodtable_name, prodtable_id, bq_creds)

#logging_warning('Error with getting products')
logging.info('----- Getting Product Details -----')
productdetails.main(chrome_path, project, dataset, prodDtable_name, prodDtable_id, bq_creds)


#logging_warning('Error with getting product details')

logging.shutdown()