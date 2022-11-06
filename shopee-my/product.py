from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from time import sleep
import pandas as pd
import logging
from datetime import datetime, date
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import io
from tqdm import tqdm 
import os 

chrome_path = "./chromedriver.exe"
project = 'shopee-mr' #config
dataset = 'shopee_my' #config
table_name = 'Product'
table_id = f'{project}.{dataset}.{table_name}' #config
bq_creds = "shopee-mr-0ac570e2c1c3.json" #config
debug_log_path = 'log/product_debug.log'
info_log_path = 'log/product_info.log'


def init_log(debug_log_path, info_log_path):
    debug_log = logging.FileHandler(debug_log_path, mode='a',encoding="utf-8")
    debug_log.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    debug_log.setFormatter(formatter)
    logging.getLogger('').addHandler(debug_log)

    info_log = logging.FileHandler(info_log_path, mode='a',encoding="utf-8")
    info_log.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    info_log.setFormatter(formatter)
    logging.getLogger('').addHandler(info_log)

    logging.info("Product Log Started")


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
        logging.exception(f"Failed. Exporting dataset to prod_df.csv at {os.getcwd()} \n {job.errors}")
        df.to_csv("prod_df.csv")
        #logging.info(job.errors)


def get_subcat_df(project, dataset, client): 
    table_id = f'{project}.{dataset}.SubCategory'
    sql = f"""
        SELECT * FROM {table_id}
        """

    subcat_df = client.query(sql).to_dataframe()
    return subcat_df


def check_duplicates(project, dataset, subcat_df, table_name, table_id, client): #compare: if subcat exists product_df within this month: remove from subcat_df
    day = date.today().replace(day=1)
    sql = f"""
        SELECT DISTINCT Category FROM {table_id} WHERE RetrievalDate >= DATE("{day}")
        """

    prod_df = client.query(sql).to_dataframe()
    category = prod_df['Category'].values.tolist()
    subcat_df = subcat_df[~subcat_df.SubCatID.isin(category)]
    return subcat_df


def get_monthlySales(driver, k):
    try:                                         
        sales = (driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[4]/div[2]/div/div[2]/div[{k}]/a/div/div/div[2]/div[3]/div[3]').text.split(' ')[0])
        if sales[-1] == "k":
            sales = int(float(sales.replace('k', ''))*1000)

        else:
            sales = int(sales)
    except:
        sales=0 

    return sales

def get_prices(driver, k):
    price = 0
    try:                                                
        originalprice = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div[1]/div[4]/div[2]/div/div[2]/div[{k}]/a/div/div/div[2]/div[2]/div[1]').text.split(' ')[0].replace('RM', '')
        if ',' in originalprice:                          
            originalprice = float(originalprice.replace(',',''))
    
        else: 
            originalprice = float(driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div[1]/div[4]/div[2]/div/div[2]/div[{k}]/a/div/div/div[2]/div[2]/div[1]').text.split(' ')[0].replace('RM', ''))

    except: 
        try:                                                
            originalprice = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[4]/div[2]/div/div[2]/div[{k}]/a/div/div/div[2]/div[2]/div[1]').text.split(' ')[0].replace('RM', '')
            if ',' in originalprice:                          
                originalprice = float(originalprice.replace(',',''))

            else: 
                originalprice = float(driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[4]/div[2]/div/div[2]/div[{k}]/a/div/div/div[2]/div[2]/div[1]').text.split(' ')[0].replace('RM', ''))

        except:
            originalprice = 0
                                                        
    try:                                                
        endprice = float(driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[4]/div[2]/div/div[2]/div[{k}]/a/div/div/div[2]/div[2]/div[1]/span[4]').text.split(' ')[0].replace('RM', ''))
        price = (originalprice + endprice)/2
        if ',' in price:
            price = float(price.replace(',',''))
                                                
    except: 
        try:                                                
            endprice = float(driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[4]/div[2]/div/div[2]/div[{k}]/a/div/div/div[2]/div[2]/div/span[4]').text.split(' ')[0].replace('RM', ''))                
            price = (originalprice + endprice)/2
            if ',' in price:
                price = float(price.replace(',',''))

        except:        
            try:                                       
                price = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[4]/div[2]/div/div[2]/div[{k}]/a/div/div/div[2]/div[2]/div/span[2]').text.split(' ')[0]
                if ',' in price:
                    price = float(price.replace(',',''))
                
                else:
                    price = float(price)
            except:    
                try:                                       
                    sleep(0.3)                              
                    price = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[4]/div[2]/div/div[2]/div[{k}]/a/div/div/div[2]/div[2]/div[1]/span[2]').text.split(' ')[0]
                    if ',' in price:
                        price = float(price.replace(',',''))
                    
                    else:
                        price = float(price)
                except:
                    logging.exception('Price not found')


    return originalprice, price


def get_prodDetails(driver, k):
    try:                                       
        name = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div[1]/div[4]/div[2]/div/div[2]/div[{k}]/a/div/div/div[2]/div[1]/div[1]/div').text
        
    except:                                 
        try:                                        
            name = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[4]/div[2]/div/div[2]/div[{k}]/a/div/div/div[2]/div[1]/div/div').text
        except:                                         
            try:                                       
                name = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[4]/div[2]/div/div[2]/div[{k}]/a/div/div/div[2]/div[1]/div[1]/div').text

            except: 
                logging.exception('Cannot find product name')


    purl =  driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[4]/div[2]/div/div[2]/div[{k}]/a').get_attribute('href')
    #logging.info(f"{name}: {purl}")          
    location = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[4]/div[2]/div/div[2]/div[{k}]/a/div/div/div[2]/div[4]').text
    pid = int(purl.split('.')[-1].split('?')[0])
    position = int(purl.split('=')[-1])+1

    return name, purl, location, pid, position


def check_popup(driver):
    try:
        button = driver.find_element_by_xpath("//button[contains(text(), 'English')]")
        button.click()

    except:
        pass


def get_products(project, dataset, table_name, table_id, bq_creds, chrome_path):
    prodsales = []
    prodorgprice = []
    prodprice = []
    prodname = []
    produrl = []
    prodlocation = []
    prodcat = []
    prodid = []
    prodpos = []
    prodrev = []

    retrievaldate = datetime.today().strftime("%Y-%m-%d")

    logging.info("Initializing Bigquery Client")
    client = init_bq(bq_creds, project)
    try:
        logging.info("Getting SubCategory DataFrame")
        subcat_df = get_subcat_df(project, dataset, client)
        logging.info(f"{len(subcat_df)} subcategories are found")

        logging.info("Checking for existing subcategories in products")
        subcat_df = check_duplicates(project, dataset, subcat_df, table_name, table_id, client)
        logging.info(f"{len(subcat_df)} subcategories are left")

        if len(subcat_df)>0:
            logging.info("Starting Chrome driver")
            driver = webdriver.Chrome(chrome_path)
            driver.maximize_window()
            for i in range(len(subcat_df)): #for each category
                logging.info(f"getting category #{i+1}/{len(subcat_df)}: {subcat_df.iloc[i]['SubCatName']} at {subcat_df.iloc[i]['SubCatURL']}")
                category = subcat_df.iloc[i]['SubCatID']
                for j in range(0, 5): #for each page in category
                    catpageurl = subcat_df.iloc[i]['SubCatURL'] + f"?page={j}&sortBy=sales"
                    logging.info(f"getting page {j+1}/5 at {catpageurl}")

                    driver.get(catpageurl)
                    check_popup(driver)
                    sleep(2)
                    html = driver.find_element_by_tag_name('html')
                    html.send_keys(Keys.PAGE_DOWN)
                    
                    html.send_keys(Keys.PAGE_DOWN)

                    #for k in tqdm(range(1, 61)): #for each product in page
                    for k in range(1, 61):
                        logging.info(f"getting product #{k}")
                        try:                                
                            driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[4]/div[2]/div/div[2]/div[{k}]')
                            try:
                                if k%10==0:
                                    html = driver.find_element_by_tag_name('html')
                                    html.send_keys(Keys.PAGE_DOWN)
                                    html.send_keys(Keys.PAGE_DOWN)

                                    sleep(3)
                                msales = get_monthlySales(driver, k)
                                prodsales.append(msales)

                                originalprice, price = get_prices(driver, k)
                                prodorgprice.append(originalprice)
                                prodprice.append(price)

                                revenue = round(float(msales) * price, 2)
                                prodrev.append(revenue)

                                name, url, location, pid, position = get_prodDetails(driver, k)
                                prodname.append(name)
                                produrl.append(url)
                                prodlocation.append(location)
                                prodid.append(pid)
                                prodpos.append(position)
                                prodcat.append(category)

                            except NoSuchElementException as e:
                                logging.exception(e)
                                logging.exception('Error')
                                break
                        except:
                            logging.exception("Item cannot be found")

            prod_df = pd.DataFrame()
            prod_df['ProdSales'] = prodsales
            prod_df['ProdOrgprice'] = prodorgprice
            prod_df['ProdPrice'] = prodprice
            prod_df['ProdName'] = prodname
            prod_df['ProdURL'] = produrl
            prod_df['ProdLocation'] = prodlocation
            prod_df['Category'] = prodcat
            prod_df['ProdID'] = prodid
            prod_df['ProdPos'] = prodpos
            prod_df['Revenue'] = prodrev
            prod_df['RetrievalDate'] = retrievaldate

            # logging.warning(prod_df)
            job = post_bigquery(prod_df, project, dataset, table_name, table_id, bq_creds)  
            return driver

        else:
            logging.info('All products have been added')
    
    except:
        logging.exception('Failed to get Big Query SubCategory Data')



def main(chrome_path, project, dataset, table_name, table_id, bq_creds):
    init_log(debug_log_path, info_log_path)
    
    try:
        driver = get_products(project, dataset, table_name, table_id, bq_creds, chrome_path)
        driver.quit()
    except:
        pass


if __name__ == "__main__":
    logging.basicConfig(filename=debug_log_path, level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    main(chrome_path, project, dataset, table_name, table_id, bq_creds) 


