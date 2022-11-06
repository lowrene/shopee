from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from time import sleep
import pandas as pd
import logging
from google.cloud import bigquery 
from google.oauth2 import service_account
import json
import io
import os

chrome_path = "./chromedriver.exe"
project = 'shopee-mr' #config
dataset = 'shopee_sg' #config
table_name = 'Category'
table_id = f'{project}.{dataset}.{table_name}' #config
bq_creds = "shopee-mr-0ac570e2c1c3.json" #config
shopee_url = 'https://shopee.sg'
debug_log_path = 'log/categories_debug.log'
info_log_path = 'log/categories_info.log'

def init_log(debug_log_path, info_log_path):
    debug_log = logging.FileHandler(debug_log_path, mode='a')
    debug_log.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    debug_log.setFormatter(formatter)
    logging.getLogger('').addHandler(debug_log)

    info_log = logging.FileHandler(info_log_path, mode='a')
    info_log.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    info_log.setFormatter(formatter)
    logging.getLogger('').addHandler(info_log)

    logging.info("Category Log Started")


def init_bq(bq_creds, project):
    creds = service_account.Credentials.from_service_account_file(
            bq_creds, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

    client = bigquery.Client(project,creds)
    return client 

def post_bigquery(df, project, dataset, table_name, table_id, bq_creds):

    client = init_bq(bq_creds, project)

    df.columns = df.columns.str.replace(' ', '')
    table_ref = client.dataset(dataset).table(table_name)
    table = client.get_table(table_ref)
    json_data = df.to_json(orient='records', lines=True)
    stringio_data = io.StringIO(json_data)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job = client.load_table_from_file(stringio_data, table, job_config = job_config)
    try:
        logging.info(job.result())
        logging.info(f'Success: Loaded {job.output_rows} rows into {table_id}.')
        return job

    except:
        logging.info(f"Failed. Exporting dataset to df.csv at {os.getcwd()}")
        df.to_csv("df.csv")


def check_duplicates(project, dataset, table_name, table_id, client): #if data exists: remove from df
    sql = f"""
        SELECT 
            * 
        FROM 
            {table_id} 
    """

    df = client.query(sql).to_dataframe()
    testdf = df[~df.CatID.isin(df.CatID)]
    return df, testdf   


def get_categories(driver, project, dataset, table_name, table_id, bq_creds, shopee_url):
    catname = []
    caturl = []
    catid = []
    sleep(3)

    logging.info("Initializing Bigquery Client")
    client = init_bq(bq_creds, project)

    logging.info("Checking for existing category details")
    df, testdf = check_duplicates(project, dataset, table_name, table_id, client)

    if len(df) != 0 and len(testdf) == 0:
        logging.info('All category data has been added')
        
    elif len(df) == 0: 
        driver.get(shopee_url)
        cats = driver.find_elements_by_class_name("home-category-list__category-grid")
        if len(cats)==0:
            driver.find_element_by_tag_name('html').click()
        cats = driver.find_elements_by_class_name("home-category-list__category-grid")
        for element in cats:
            url = element.get_attribute('href')
            caturl.append(url)
            cid = int(url.split('.')[-1])
            catid.append(cid)
            name = url.split('/')[-1].split('.')[0].replace('-', ' ').replace(' cat', '')
            catname.append(name)
            # logging.warning(f'url: {url}, id: {cid}, name: {name}')

        driver.quit()

        df = pd.DataFrame()
        df['CatName'] = catname
        df['CatURL'] = caturl
        df['CatID'] = catid

        job = post_bigquery(df, project, dataset, table_name, table_id, bq_creds)
        return df

    else:
        logging.info('error')


def main(chrome_path, project, dataset, table_name, table_id, bq_creds, shopee_url):
    driver = webdriver.Chrome(chrome_path)
    df = get_categories(driver, project, dataset, table_name, table_id, bq_creds, shopee_url)
    return df


if __name__ == "__main__":
    logging.basicConfig(filename=debug_log_path, level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    main(chrome_path, project, dataset, table_name, table_id, bq_creds, shopee_url) 


