from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from time import sleep
import pandas as pd
import numpy as np
import logging
from google.cloud import bigquery 
from google.oauth2 import service_account
import json
import io
import os
from tqdm import tqdm
from datetime import datetime, date

chrome_path = "./chromedriver.exe"
project = 'shopee-mr' #config
dataset = 'shopee_my' #config
table_name = 'ProductDetails'
table_id = f'{project}.{dataset}.{table_name}' #config
bq_creds = "shopee-mr-0ac570e2c1c3.json" #config
debug_log_path = 'log/productDetails_debug.log'
info_log_path = 'log/productDetails_info.log'


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

    logging.info("Product Details Log Started")


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
        logging.exception(f"Failed. Exporting dataset to proddetails_df.csv at {os.getcwd()} \n {job.errors}")
        print(job.errors)
        df.to_csv("proddetails_df.csv")
        #logging.info(job.errors)


def get_prod_df(project, dataset, client, subcatid): #for 1 subcat FOR THIS MONTH: WHERE
    day = date.today().replace(day=1)
    table_id = f'{project}.{dataset}.Product'
    sql = f"""
        SELECT * FROM {table_id} WHERE RetrievalDate >= DATE("{day}") AND Category = {subcatid}
        """

    prod_df = client.query(sql).to_dataframe()
    return prod_df

def get_subcat_df(project, dataset, client): 
    table_id = f'{project}.{dataset}.SubCategory'
    sql = f"""
        SELECT * FROM {table_id}
        """

    subcat_df = client.query(sql).to_dataframe()

    return subcat_df

# DEF FUNCTION TO GET PRODDETAILS TABLE
def get_proddetails_df(table_id, client):
    day = date.today().replace(day=1)
    sql = f"""
        SELECT DISTINCT ItemID FROM {table_id} WHERE ProductDate >= DATE("{day}")
        """

    proddetails_df = client.query(sql).to_dataframe()
    return proddetails_df


def check_duplicates(prod_df, proddetails_df): #compare: if subcat exists product_df within this month: remove from subcat_df
    items = proddetails_df['ItemID'].values.tolist()
    prod_df = prod_df[~prod_df.ProdID.isin(items)]
    return prod_df

def check_product_exists(driver, produrl):
    try:
        if driver.current_url != produrl:
            logging.info("product redirected")
            return False
        element = driver.find_element_by_xpath('//*[contains(text(), "The product doesn")]')
        if element.text == "The product doesn't exist":
            # logging.info("False")
            return False
        else:
            return True
    except:
        # logging.info('True')
        return True


def get_ratings(driver):
    try:
        averating = float(driver.find_element_by_class_name('OitLRu').text)
        totalrating = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[2]/div[2]/div[2]/div[2]/div[3]/div/div[2]/div[2]/div[1]').text
        if totalrating[-1] == "k":                    
            totalrating = int(float(totalrating.replace('k', ''))*1000)

        else:
            totalrating = int(totalrating)
    except:
        averating = float(0)
        totalrating = 0

    return averating, totalrating


def get_quantity_sales(driver):
    try:
        quantity = int(driver.find_element_by_xpath('//*[contains(text(), "piece available")]').text.split(' ')[0])
        tsales = driver.find_element_by_class_name('aca9MM').text
        if tsales[-1] == "k":
            tsales = int(float(tsales.replace('k', ''))*1000)

        else:
            tsales = int(tsales)
    except:
        try:
            sleep(1)
            quantity = int(driver.find_element_by_xpath('//*[contains(text(), "piece available")]').text.split(' ')[0])
            tsales = driver.find_element_by_class_name('aca9MM').text
            if tsales[-1] == "k":
                tsales = int(float(tsales.replace('k', ''))*1000)

            else:
                tsales = int(tsales)
        except:
            quantity=0
            tsales=0

    return quantity, tsales


def get_catDetails(driver, produrl, i):
    specs = ''
    brandvalue = ''
    categorylist = ''

    try:
        title = driver.find_element_by_xpath("//div[contains(text(), 'Product Specifications')]")
        labels = title.find_element_by_xpath('..').find_elements_by_xpath('.//label')
        values = title.find_element_by_xpath('..').find_elements_by_xpath('.//div/div/div')
    except:
        return np.nan, np.nan, np.nan

    try:
        if len(labels)<=2:
            for k in range(len(labels)): 
                if labels[k].text == 'Category':
                    categorylist = values[k].text.replace('\n', '>')
                    
                    
                elif labels[k].text == 'Brand':
                    brandvalue = labels[k].find_element_by_xpath('..').find_element_by_xpath('.//a').text
                    
                    
                else:
                    if labels[1].text == 'Brand':
                        specs = specs + labels[k].text + ": " + values[k-1].text + '\n'

                    else:
                        specs = specs + labels[k].text + ": " + values[k].text + '\n'
        
        else:
            for k in range(len(labels)): 
                if labels[k].text == 'Category':
                    categorylist = values[k].text.replace('\n', '>')
                    
                    
                elif labels[k].text == 'Brand':
                    brandvalue = labels[k].find_element_by_xpath('..').find_element_by_xpath('.//a').text
                    
                    
                else:
                    if labels[1].text == 'Brand':
                        specs = specs + labels[k].text + ": " + values[k-1].text + '\n'

                    elif labels[2].text == 'Brand':
                        specs = specs + labels[k].text + ": " + values[k-1].text + '\n'

                    else:
                        specs = specs + labels[k].text + ": " + values[k].text + '\n'
                        

        return categorylist, brandvalue, specs
        
    except:
        logging.exception(f'Error in getting product {i} specs at {produrl}')
        return np.nan, np.nan, np.nan


def check_legalpopup(driver):
    try:
        button = driver.find_element_by_xpath("//button[contains(text(), 'I AM OVER 18')]")
        button.click()

    except:
        pass

def check_popup(driver):
    try:
        button = driver.find_element_by_xpath("//button[contains(text(), 'English')]")
        button.click()

    except:
        pass


def check_21popup(driver):
    try:
        button = driver.find_element_by_xpath("//button[contains(text(), 'YES I AM')]")
        button.click()

    except:
        pass



def get_prodDetails(project, dataset, table_name, table_id, bq_creds, chrome_path):
    
    logging.info("Initializing Bigquery Client")
    client = init_bq(bq_creds, project)
    #initialize driver
    logging.info("Starting Chrome driver")
    driver = webdriver.Chrome(chrome_path)
    
    try:
        #GET PRODDETAILS TABLE FOR THIS MONTH: PRODDETAILS_DF
        proddetails_df = get_proddetails_df(table_id, client)
        subcat_df = get_subcat_df(project, dataset, client) #get subcat dataframe
        for i in range(len(subcat_df)): #for each subcat
            logging.info(f"Getting Products DataFrame for subcat {subcat_df.iloc[i]['SubCatName']} at {subcat_df.iloc[i]['SubCatID']}")
            subcatid = subcat_df.iloc[i]['SubCatID']

            prod_df = get_prod_df(project, dataset, client, subcatid) #input subcatID
            logging.info(f"{len(prod_df)} products are found")

            logging.info("Checking for existing product details")
            prod_df = check_duplicates(prod_df, proddetails_df) #INPUT PROD_DF AND PRODDETAILS_DF
            logging.info(f"{len(prod_df)} products are left")

            if len(prod_df)>0:
                prodaverating = []
                prodtotalrating = []
                prodquantity = []
                totalsales = []
                itemid = []
                proddate = []
                catlist = []
                brands = []
                prodspec = []
                driver.maximize_window()
                #logging.info(prod_df)
                for i in tqdm(range(len(prod_df))):
                    #logging.debug(f"getting {prod_df.iloc[i]['ProdName']} at {prod_df.iloc[i]['ProdURL']}") 
                    #logging.info(f"getting product #{i+1}")
                    specs = ''
                    produrl = prod_df.iloc[i]['ProdURL']
                    driver.get(produrl) 
                    sleep(2)
                    if check_product_exists(driver, produrl):
                        try:
                            #logging.info("Product exists")
                            check_legalpopup(driver)
                            check_21popup(driver)
                            check_popup(driver)
                            html = driver.find_element_by_tag_name('html')
                            html.send_keys(Keys.PAGE_DOWN)

                            html.send_keys(Keys.PAGE_DOWN)
                            sleep(2)
                            aveRating, numRating = get_ratings(driver)
                            prodaverating.append(aveRating)
                            prodtotalrating.append(numRating)
                            # print(type(aveRating), type(numRating))
                            # logging.warning(f'ave rating {aveRating}, total rating {numRating}')

                            quantity, tsales = get_quantity_sales(driver)
                            prodquantity.append(quantity)
                            totalsales.append(tsales)
                            # logging.warning(f'quantity ava: {quantity}, total sales: {tsales}')

                            categorylist, brandvalue, specs = get_catDetails(driver, produrl, i)
                            catlist.append(categorylist)
                            
                            prodspec.append(specs)
                           
                            if brandvalue == '':
                                brands.append(np.nan)
                            else:
                                brands.append(brandvalue)
                            # if len(prodspec) != len(brands):
                            #     brands.append(np.nan)

                            prodid = int(produrl.split('.')[-1].split('?')[0])
                            itemid.append(prodid)
                            date = prod_df.iloc[i]['RetrievalDate']
                            date = date.strftime("%Y-%m-%d")
                            proddate.append(date)

                            # logging.warning(f'catlist: {categorylist}, brand: {brands}, specs: {specs}')
                            sleep(2)
                        except:
                            logging.exception(f"Fail to find product details of #{i} {prod_df.iloc[i]['ProdName']}")
                            continue

                    else:
                        logging.info("Product does not exist")
                        prodaverating.append(np.nan)
                        prodtotalrating.append(np.nan)
                        prodquantity.append(np.nan)
                        catlist.append(np.nan)
                        brands.append(np.nan)
                        prodspec.append(np.nan)
                        totalsales.append(np.nan)
                        prodid = int(produrl.split('.')[-1].split('?')[0])
                        itemid.append(prodid)
                        date = prod_df.iloc[i]['RetrievalDate']
                        date = date.strftime("%Y-%m-%d")
                        proddate.append(date)

                proddetail_df = pd.DataFrame()
                proddetail_df['AveRating'] = prodaverating
                proddetail_df['NumRating'] = prodtotalrating
                proddetail_df['Quantity'] = prodquantity
                proddetail_df['CatList'] = catlist
                proddetail_df['Brand'] = brands
                proddetail_df['ProdSpecs'] = prodspec
                proddetail_df['TotalSales'] = totalsales
                proddetail_df['ItemID'] = itemid
                proddetail_df['ProductDate'] = proddate
                proddetail_df = proddetail_df.astype({'NumRating': 'Int64', 'Quantity': 'Int64', 'TotalSales': 'Int64'})
                #prod = prod_df.merge(proddetail_df, how="left", left_on="ProdID", right_on="ItemID")
                job = post_bigquery(proddetail_df, project, dataset, table_name, table_id, bq_creds)  
            
            else:
                logging.info('All product details has been added')

    except:
        logging.exception('Failed to get Big Query Products')

    driver.quit()
    

def main(chrome_path, project, dataset, table_name, table_id, bq_creds):
    get_prodDetails(project, dataset, table_name, table_id, bq_creds, chrome_path)
    


if __name__ == "__main__":
    logging.basicConfig(filename=debug_log_path, level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    main(chrome_path,project, dataset, table_name, table_id, bq_creds) 


