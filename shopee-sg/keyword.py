from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from time import sleep
import pandas as pd
import numpy as np
import productdetails
from tqdm import tqdm
import logging
from datetime import datetime, date
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import io
import os

chrome_path = "./chromedriver.exe"
project = 'shopee-mr' #config
dataset = 'shopee_sg' #config
table_name = 'Keyword'
table_id = f'{project}.{dataset}.{table_name}' #config
bq_creds = "shopee-mr-0ac570e2c1c3.json" #config
debug_log_path = 'log/keyword_debug.log'
info_log_path = 'log/keyword_info.log'

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

    logging.info("Keyword Log Started")


def init_bq(bq_creds, project):
    creds = service_account.Credentials.from_service_account_file(
            bq_creds, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

    client = bigquery.Client(project,creds)
    return client 


def post_bigquery(df, project, dataset, table_name, table_id, bq_creds, retrievaldate, keyword):

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
        logging.exception(f"Failed. Exporting dataset to {keyword} {retrievaldate}.csv at {os.getcwd()} \n {job.errors}")
        df.to_csv(f"{keyword} {retrievaldate}.csv")
        #logging.info(job.errors)



def check_duplicates(project, dataset, table_name, table_id, client): 
    day = date.today().replace(day=1)
    sql = f"""
        SELECT * FROM {table_id} WHERE RetrievalDate >= DATE("{day}")
        """

    testdf = client.query(sql).to_dataframe()
    word = testdf['Keyword'].tolist()
    
    return word



def get_monthlySales(driver, k):
    try:                                        
        sales = (driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[3]/div[2]/div[{k}]/a/div/div/div[2]/div[3]/div[3]').text.split(' ')[0])
        if sales[-1] == "k":
            sales = int(float(sales.replace('k', ''))*1000)

        else:
            sales = int(sales)
    except:
        try:                                        
            sales = (driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[2]/div[2]/div[{k}]/a/div/div/div[2]/div[3]/div[3]').text.split(' ')[0])
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
        originalprice = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[3]/div[2]/div[{k}]/a/div/div/div[2]/div[2]/div[1]').text.split(' ')[0].replace('$', '')
        if ',' in originalprice:                          
            originalprice = float(originalprice.replace(',',''))
    
        else: 
            originalprice = float(driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[3]/div[2]/div[{k}]/a/div/div/div[2]/div[2]/div[1]').text.split(' ')[0].replace('$', ''))

    except: 
        try:                                                
            originalprice = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[3]/div[2]/div[{k}]/a/div/div/div[2]/div[2]/div/span[2]').text.split(' ')[0].replace('$', '')
            if ',' in originalprice:                          
                originalprice = float(originalprice.replace(',',''))

            else: 
                originalprice = float(driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[3]/div[2]/div[{k}]/a/div/div/div[2]/div[2]/div/span[2]').text.split(' ')[0].replace('$', ''))

        except:
            try:
                originalprice = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[2]/div[2]/div[{k}]/a/div/div/div[2]/div[2]/div[1]').text.split(' ')[0].replace('$', '')
            except:
                originalprice = 0

    try: 
        endprice = float(driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[4]/div[2]/div/div[2]/div[{k}]/a/div/div/div[2]/div[2]/div[1]/span[4]').text.split(' ')[0].replace('$', ''))
        price = (originalprice + endprice)/2
        if ',' in price:
            price = float(price.replace(',',''))
                                                
    except:      
        try:                                               
            price = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[3]/div[2]/div[{k}]/a/div/div/div[2]/div[2]/div[1]/span[4]').text.split(' ')[0]
            if ',' in price:
                price = float(price.replace(',',''))
            
            else:
                price = float(price)
        except:
            try:                                                
                price = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[3]/div[2]/div[{k}]/a/div/div/div[2]/div[2]/div[2]/span[2]').text.split(' ')[0]
                if ',' in price:
                    price = float(price.replace(',',''))
                
                else:
                    price = float(price)

            except:
                try:                                              
                    price = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[3]/div[2]/div[{k}]/a/div/div/div[2]/div[2]/div/span[2]').text.split(' ')[0]
                    if ',' in price:
                        price = float(price.replace(',',''))
                    
                    else:
                        price = float(price)
                except:
                    try:                                            
                        price = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[2]/div[2]/div[{k}]/a/div/div/div[2]/div[2]/div[2]/span[2]').text.split(' ')[0]
                        if ',' in price:
                            price = float(price.replace(',',''))
                        
                        else:
                            price = float(price)
                    except:
                        try:                                              
                            price = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[2]/div[2]/div[{k}]/a/div/div/div[2]/div[2]/div/span[4]').text.split(' ')[0]   
                            if ',' in price:
                                price = float(price.replace(',',''))
                            
                            else:
                                price = float(price)

                        except:
                            try:
                                price = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[2]/div[2]/div[{k}]/a/div/div/div[2]/div[2]/div/span[2]').text.split(' ')[0] 
                                if ',' in price:
                                    price = float(price.replace(',',''))
                                
                                else:
                                    price = float(price)

                            except:
                                logging.info('Price not found')
                                     
        

    return originalprice, price


def get_prodDetails(driver, k):               
    try:                                      
        name = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[3]/div[2]/div[{k}]/a/div/div/div[2]/div[1]/div[1]/div').text
        
    except:      
        try:                                       
            name = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[2]/div[2]/div[{k}]/a/div/div/div[2]/div[1]/div[1]/div').text  
        except:               
            try: 
                name = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[3]/div[2]/div[{k}]/a/div/div/div[2]/div[1]/div/div').text  
            except:       
                logging.info('Cannot find product name')


    try:                                        
        purl = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[3]/div[2]/div[{k}]/a').get_attribute('href')
    except:
        try:
            purl = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[2]/div[2]/div[{k}]/a').get_attribute('href')
        except:
            logging.info('Cannot find product URL')


    try:                                            
        location = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[3]/div[2]/div[{k}]/a/div/div/div[2]/div[4]').text
    except:
        try:
            location = driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[2]/div[2]/div[{k}]/a/div/div/div[2]/div[4]').text
        except:
            logging.info('Cannot find product location')


    pid = int(purl.split('.')[-1].split('?')[0])
    position = int(purl.split('=')[-1])+1

    return name, purl, location, pid, position


def get_AllProd(driver, keyword, retrievaldate):
    prodsales = []
    prodorgprice = []
    prodprice = []
    prodname = []
    produrl = []
    prodlocation = []
    prodid = []
    prodpos = []
    prodrev = []
    

    logging.info("------ Getting Products ------")
    for i in range(0, 5): #for each page
        logging.info(f"getting page {i+1}/5")

        url = f"https://shopee.sg/search?keyword={keyword}&page={i}&sortBy=sales"
        driver.get(url)

        driver.refresh()
        sleep(3)
        html = driver.find_element_by_tag_name('html')
        html.send_keys(Keys.PAGE_DOWN)
        
        html.send_keys(Keys.PAGE_DOWN)
        
        for k in tqdm(range(1, 61)): #for each product in page
            try:                                                
                driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div[3]/div[2]/div[{k}]')
                
                try:
                    if k%10==0:
                        html = driver.find_element_by_tag_name('html')
                        html.send_keys(Keys.PAGE_DOWN)

                    msales = get_monthlySales(driver, k)
                    prodsales.append(msales)
                    # logging.info(f'sales {msales}')

                    originalprice, price = get_prices(driver, k)
                    prodorgprice.append(originalprice)
                    prodprice.append(price)
                    # logging.info(f'org price: {originalprice}, price: {price}')

                    revenue = round(float(msales) * price, 2)
                    prodrev.append(revenue)
                    # logging.info(f'revenue: {revenue}')

                    name, url, location, pid, position = get_prodDetails(driver, k)
                    prodname.append(name)
                    produrl.append(url)
                    prodlocation.append(location)
                    prodid.append(pid)
                    prodpos.append(position)
                    # logging.info(f'name: {name}, url: {url}, location: {location}, pid: {pid}, position: {position}')


                except NoSuchElementException as e:
                    logging.exception(e)
                    logging.exception('Error')
                    break

            except:
                try:                    
                    driver.find_element_by_xpath(f'/html/body/div[1]/div/div[3]/div/div[2]/div/div[2]/div[{k}]')

                    try:
                        if k%10==0:
                            html = driver.find_element_by_tag_name('html')
                            html.send_keys(Keys.PAGE_DOWN)

                        msales = get_monthlySales(driver, k)
                        prodsales.append(msales)
                        # logging.info(f'sales {msales}')

                        originalprice, price = get_prices(driver, k)
                        prodorgprice.append(originalprice)
                        prodprice.append(price)
                        # logging.info(f'org price: {originalprice}, price: {price}')

                        revenue = round(float(msales) * price, 2)
                        prodrev.append(revenue)
                        # logging.info(f'revenue: {revenue}')

                        name, url, location, pid, position = get_prodDetails(driver, k)
                        prodname.append(name)
                        produrl.append(url)
                        prodlocation.append(location)
                        prodid.append(pid)
                        prodpos.append(position)
                        # logging.info(f'name: {name}, url: {url}, location: {location}, pid: {pid}, position: {position}')


                    except NoSuchElementException as e:
                        logging.exception(e)
                        logging.exception('Error')
                        break

                except:    
                    logging.exception("Item cannot be found")

           
    keywordProd_df = pd.DataFrame()
    keywordProd_df['ProdSales'] = prodsales
    keywordProd_df['ProdOrgprice'] = prodorgprice
    keywordProd_df['ProdPrice'] = prodprice
    keywordProd_df['ProdName'] = prodname
    keywordProd_df['ProdURL'] = produrl
    keywordProd_df['ProdLocation'] = prodlocation
    keywordProd_df['ProdID'] = prodid
    keywordProd_df['ProdPos'] = prodpos
    keywordProd_df['Revenue'] = prodrev
    keywordProd_df['RetrievalDate'] = retrievaldate
    keywordProd_df['Keyword'] = keyword
    keywordProd_df.to_csv("keywordProd_df.csv")
    return keywordProd_df
    

def get_AllProdDetails(driver, keywordProd_df):
    prodaverating = []
    prodtotalrating = []
    prodquantity = []
    totalsales = []
    itemid = []
    catlist = []
    brands = []
    prodspec = []


    logging.info("------ Getting Product Details ------")
    # logging.info(keywordProd_df['ProdURL'])
    for i in tqdm(range(len(keywordProd_df))):
        specs = ''
        produrl = keywordProd_df.iloc[i]['ProdURL']
        driver.get(produrl) 
        sleep(2)
        if productdetails.check_product_exists(driver, produrl):
            try:
                #logging.info("Product exists")
                html = driver.find_element_by_tag_name('html')
                html.send_keys(Keys.PAGE_DOWN)

                html.send_keys(Keys.PAGE_DOWN)

                aveRating, numRating = productdetails.get_ratings(driver)
                prodaverating.append(aveRating)
                prodtotalrating.append(numRating)
                # print(type(aveRating), type(numRating))
                # logging.warning(f'ave rating {aveRating}, total rating {numRating}')

                quantity, tsales = productdetails.get_quantity_sales(driver)
                prodquantity.append(quantity)
                totalsales.append(tsales)
                # logging.warning(f'quantity ava: {quantity}, total sales: {tsales}')

                categorylist, brandvalue, specs = productdetails.get_catDetails(driver, produrl, i)
                catlist.append(categorylist)
                brands.append(brandvalue)
                prodspec.append(specs)
                if brandvalue == '':
                    brands.append(np.nan)
                else:
                    brands.append(brandvalue)

                prodid = int(produrl.split('.')[-1].split('?')[0])
                itemid.append(prodid)
                # logging.warning(f'catlist: {catlist}, brands: {brands}, specs: {prodspec}')
                sleep(2)
            except:
                logging.exception(f"Fail to find product details of #{i} {keywordProd_df.iloc[i]['ProdName']}")
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

            
    keywordProdDetails_df = pd.DataFrame()
    keywordProdDetails_df['AveRating'] = prodaverating
    keywordProdDetails_df['NumRating'] = prodtotalrating
    keywordProdDetails_df['Quantity'] = prodquantity
    keywordProdDetails_df['CatList'] = catlist
    keywordProdDetails_df['Brand'] = brands
    keywordProdDetails_df['ProdSpecs'] = prodspec
    keywordProdDetails_df['TotalSales'] = totalsales
    keywordProdDetails_df['ItemID'] = itemid
    # keywordProdDetails_df = keywordProdDetails_df.astype({'NumRating': 'Int64', 'Quantity': 'Int64', 'TotalSales': 'Int64'})
    return keywordProdDetails_df


def main(chrome_path, project, dataset, table_name, table_id, bq_creds):
    logging.info("Initializing Bigquery Client")
    client = init_bq(bq_creds, project)

    logging.info("Starting Chrome driver")
    driver = webdriver.Chrome(chrome_path)
    driver.maximize_window()

    retrievaldate = datetime.today().strftime("%Y-%m-%d")
    keyword = input("Enter keyword you wish to search:")

    word = check_duplicates(project, dataset, table_name, table_id, client) 

    if keyword in word:
        logging.info("Product is already in Big Query")


    else:
        logging.info("Searching...")

        keywordProd_df = get_AllProd(driver, keyword, retrievaldate)
        keywordProdDetails_df = get_AllProdDetails(driver, keywordProd_df)
        df = keywordProd_df.merge(keywordProdDetails_df, how="left", left_on="ProdID", right_on="ItemID")

        df.drop("ItemID", axis=1, inplace=True)

        # logging.info(df)
        job = post_bigquery(df, project, dataset, table_name, table_id, bq_creds, retrievaldate, keyword)



if __name__ == "__main__":
    init_log(debug_log_path, info_log_path)
    main(chrome_path, project, dataset, table_name, table_id, bq_creds) 