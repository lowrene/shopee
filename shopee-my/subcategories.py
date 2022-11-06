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
dataset = 'shopee_my' #config
table_name = 'SubCategory'
table_id = f'{project}.{dataset}.{table_name}' #config
bq_creds = "shopee-mr-0ac570e2c1c3.json" #config
debug_log_path = 'log/subcategories_debug.log'
info_log_path = 'log/subcategories_info.log'

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

    logging.info("SubCategory Log Started")

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
        logging.info(f"Failed. Exporting dataset to subcat_df.csv at {os.getcwd()}")
        df.to_csv("subcat_df.csv")
        #logging.info(job.errors)

# def get_df():
#     df = pd.read_csv(r'C:\Users\alexi\Desktop\Shopee files\maincat_df.csv')
#     return df

    
def get_df(project, dataset, client): 
    table_id = f'{project}.{dataset}.Category'
    sql = f"""
        SELECT 
            * 
        FROM 
            {table_id} 
    """

    df = client.query(sql).to_dataframe()
    return df
    


def check_duplicates(project, dataset, df, table_name, table_id, client): #if data exists: remove from df
    sql = f"""
        SELECT 
            * 
        FROM 
            {table_id} 
    """

    subcat_df = client.query(sql).to_dataframe()
    sid = subcat_df['ParentID'].values.tolist()
    df = df[~df.CatID.isin(sid)]
    return df


def check_popup(driver):
    try:
        button = driver.find_element_by_xpath("//button[contains(text(), 'English')]")
        button.click()

    except:
        pass

def get_subcategories(driver, df, project, dataset, table_name, table_id, bq_creds):
    subcaturl = [] 
    subcatname = []
    parentid = []
    subcatid = []

    logging.info("Initializing Bigquery Client")
    client = init_bq(bq_creds, project)
    try:
        df = get_df(project, dataset, client)
        df = check_duplicates(project, dataset, df, table_name, table_id, client)
        if len(df)>0:
            #logging.info(f'this is {df}')
            # if len(df) > 0: #if len(df)>0 after removing : run post
            for i in range(len(df)): #for each category
                catID = df.iloc[i]['CatID'] #get catid
                catpageurl = df.iloc[i]['CatURL'] #get caturl
                driver.get(catpageurl)  #go to page
                check_popup(driver)
                sleep(2)
                more_btn = driver.find_element_by_class_name('stardust-dropdown').click() #click more btn
                for element in driver.find_elements_by_class_name('shopee-category-list__sub-category'):
                    subcategoryurl = element.get_attribute('href')
                    subcaturl.append(subcategoryurl)
                    subcategoryname = subcategoryurl.split('/')[-1].split('.')[0].replace('-', ' ').replace(' cat', '')
                    subcatname.append(subcategoryname)
                    subcategoryid = subcategoryurl.split('.')[-1]
                    subcatid.append(subcategoryid)
                    parentid.append(catID)    

            driver.quit()

            subcat_df = pd.DataFrame()
            subcat_df['SubCatID'] = subcatid
            subcat_df['SubCatName'] = subcatname
            subcat_df['SubCatURL'] = subcaturl
            subcat_df['ParentID'] = parentid

            
            job = post_bigquery(subcat_df, project, dataset, table_name, table_id, bq_creds)


        else:
            logging.info('All subcategory data has been added')
    
    except:
        logging.info('Failed to get Big Query Category Data')

    

def main(chrome_path, df, project, dataset, table_name, table_id, bq_creds):

    driver = webdriver.Chrome(chrome_path)
    get_subcategories(driver, df, project, dataset, table_name, table_id, bq_creds)


if __name__ == "__main__":
    #df = get_df()
    logging.basicConfig(filename=debug_log_path, level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    main(chrome_path, df, project, dataset, table_name, table_id, bq_creds) 