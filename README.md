# Shopee-MR

## dependencies 
1. download anaconda: https://docs.anaconda.com/anaconda/install/windows/
2. ensure system is in python 3

## chromedriver installation
1. download chromedriver according to system latest chrome version: https://chromedriver.chromium.org/downloads 
2. extract zip file 
3. place the file in project folder

## pip installation
- selenium
- pyarrow
- google-cloud-bigquery

## additional libraries
- os
- io
- json
- pandas 
- numpy
- logging
- time

## bigquery setup
1. get bq credentials json file from google cloud platform (IAM & Admin > Service Account > Keys)
2. add new json key 
3. place the file in project folder

## bat file usage
Create BAT file to run scripts on Task Scheduler

##### mainCat - runs once at setup

##### mainProd - runs monthly and data stored in bigquery

##### keyword - runs on demand

## script structure
1. open Anaconda Prompt on computer
2. navigate to Shopee-MR folder

| Script           | Description                                               | How to run                             | 
| ---------------- | --------------------------------------------------------- | -------------------------------------- | 
| categories       | get main categories                                       | ```python categories.py```             |
| subcategories    | get subcategories                                         | ```python subcategories.py```          |
| mainCat          | run both categories and subcategories scripts             | ```python mainCat.py```                |
| product          | get top 300 products from each subcategory                | ```python products.py```               |
| productdetails   | get detailed product information for each product         | ```python productdetails.py```         |
| mainProd         | run both product and productdetails scripts               | ```python mainProd.py```               |
| keyword          | users can search for specific keyword                     | ```python keyword.py```                |

## categories script
#### input
1. chromedriver.exe
2. bq creds

#### output
1. console display the number of rows uploaded in Category table in shopee-mr bigquery
2. if error occurs:
- it will be logged in the console
- df.csv file will be created


## subcategories script
#### input
1. chromedriver.exe
2. bq creds

#### output
1. console display the number of rows uploaded in SubCategory table in shopee-mr bigquery
2. if error occurs:
- it will be logged in the console
- subcat_df.csv file will be created

## product script
#### description
run through each subcategory in bigquery to collect product data such as name, monthly sales, price, location and etc

#### input
1. chromedriver.exe
2. bq creds

#### output
1. validation check for existing products in each subcategory that exist in bigquery for the month
2. console display the number of rows uploaded in Product table in shopee-mr bigquery
3. if error occurs:
- it will be logged in the console
- prod_df.csv file will be created


## productdetails script
#### description
run through each product stored in bigquery for the month and collect data such as brand, catlist, product specifications, ratings, quantity available and total sales

#### input
1. chromedriver.exe
2. bq creds

#### output
1. validation check for existing product details in each subcategory that exist in bigquery for the month
2. console display the number of rows uploaded in ProductDetails table in shopee-mr bigquery
3. if error occurs:
- it will be logged in the console
- proddetails_df.csv file will be created

## keyword script
#### input
1. chromedriver.exe
2. bq creds
3. keyword input in anaconda prompt

#### output
1. console display the number of rows uploaded in Keyword table in shopee-mr bigquery
2. if error occurs:
- it will be logged in the console
- csv file will be created named 'keyword {retrievaldate}'


## dashboard
https://datastudio.google.com/u/0/reporting/b2f9b608-3069-4a75-8c98-09b74aed29eb/page/p_fk44kszvnc

## ERD 
![erd](https://user-images.githubusercontent.com/89075746/137271131-64bdad6e-3ace-412e-853e-a70890d4d09e.jpg)



