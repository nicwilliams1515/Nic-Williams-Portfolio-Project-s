import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from awsglue.dynamicframe import DynamicFrame
import pandas as pd
import requests
import re
import boto3
from warcio.archiveiterator import ArchiveIterator
from io import BytesIO
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
import argparse

def html_to_text(page):
    """Converts html page to text
    Args:
        page:  html
    Returns:
        soup.get_text(" ", strip=True):  string
    """
    try:
        encoding = EncodingDetector.find_declared_encoding(page, is_html=True)
        soup = BeautifulSoup(page, "lxml", from_encoding=encoding)
        for script in soup(["script", "style"]):
            script.extract()
        return soup.get_text(" ", strip=True)
    except:
        return ""

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

parseList = {
    'Product':[],
    'Price': [],
    'warc_path': [],
    'file_name': [],
    'file_time_span': []
    }

input_data = glueContext.create_dynamic_frame.from_catalog(database = "ccindex", table_name = "walmart_watches_query_2018")
sparkDF = input_data.toDF()

s3client = boto3.client('s3')
i = 0
dfCount = sparkDF.count()
print("dfCount", dfCount)
for row in sparkDF.take(dfCount):
    i = i + 1
    if i > dfCount:
        break
    if i % 10 == 0:
        print("i:", i)
    #print(i)
    #print("row: ", row)
    warc_path = row['warc_filename']
    file_name = warc_path.split('crawl-data/')[1].split('/')[0]
    file_time_span = warc_path.split('crawl-data/')[1].split('/')[-1]
    offset = int(row['warc_record_offset'])
    length = int(row['warc_record_length'])
    rangereq = 'bytes={}-{}'.format(offset, (offset+length-1))
    response = s3client.get_object(Bucket='commoncrawl', Key=warc_path, Range=rangereq)
    record_stream = BytesIO(response["Body"].read())
    final_product = []

    for record in ArchiveIterator(record_stream):
        page = record.content_stream().read()
        #If any Error Occures on the cached Data move to another iteration
        if b'Redirecting...' in page or b'' == page or b'Moved Permanently' in page:
            continue
        #Parse HTML
        soup = BeautifulSoup(str(page),"html.parser")
        #Select all The LI -> Possible the products as well
        products = soup.findAll('li')

        #Iterate through each LI -> Possibly Product
        for product in products:
            ele1 = product.select('.product-title-link')
            ele2 = product.select('.price')
            #IF the LI has product title and price then proceed to next
            if len(ele1) > 0 and len(ele2) > 0:
                title = ele1[0].get_text().strip()
                price = ele2[0].get_text().strip()

                #-----CHECK ALREADY ADDEDD LIST----
                found = False
                for fp in final_product:
                    if fp['Product'] == title:
                        found = True
                        break
                if found == False:
                    final_product.append({'Product': title, "Price": price})
                #-----CHECK ALREADY ADDEDD LIST END----

        #--- Add to our final list for pd
        for fp in final_product:
            parseList['Product'].append(fp['Product'])
            parseList['Price'].append(fp['Price'])
            parseList['warc_path'].append(warc_path)
            parseList['file_name'].append(file_name)
            parseList['file_time_span'].append(file_time_span)
        print("parseLength:", len(parseList))
        #move to next iteration
        continue



#print(parseList)
df = pd.DataFrame(data=parseList)
#print(df.head())
sparkDF=spark.createDataFrame(df)
processed_data=DynamicFrame.fromDF(sparkDF, glueContext, "processed_data")

glueContext.write_dynamic_frame.from_options(frame = processed_data, connection_type = "s3", connection_options = {"path": "s3://inflationtracker-bucket01/processed_data/"}, format = "parquet")
job.commit()
