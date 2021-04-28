import pymysql
import csv
import boto3
import configparser
import psycopg2

#Redshift db script part

# RedShift cluster connection
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("aws_redshift", "database")
user = parser.get("aws_redshift", "username")
passw = parser.get("aws_redshift", "password")
host = parser.get("aws_redshift", "host")
port = parser.get("aws_redshift", "port")

rs_conn = psycopg2.connect(
    "dbname=" + dbname
    + " user=" + user
    + " password=" + password
    + " host=" + host
    + " port=" + port
)

#redshift query
rs_sql = """SELECT COALESCE(MAX(LastUpdated), '1900-01-01') FROM Orders;"""

rs_cursor = rs_conn.cursor()
rs_cursor.execute(rs_sql)
result = rs_cursor.fetchone()

#only one field returned
last_update_warehouse = result[0]

#end the connections
rs_cursor.close()
rs_conn.commit()


#RDS db script part

parser= configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("mysql_config", "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config", "username")
dbname = parser.get("mysql_config", "database")
password = parser.get("mysql_config", "password")

conn = pymysql.connect(host=hostname,
                       user=username,
                       password=password,
                       db=dbname,
                       port=int(port))

if conn is None:
    print("Error connecting to the mySQL database")
else:
    print("Connection established!")

m_query = """SELECT *
            FROM Orders
            WHERE LastUpdated > %s;"""
extracted_filename = "order_extract.csv"

#extract the file using pymysql cursor's function
#takes a mysql connection and returns a csv

m_cursor = conn.cursor()
m_cursor.execute(m_query, (last_update_warehouse,))
results = m_cursor.fetchall()

#write the results of the query from the DB
#into a csv file 

with open(extracted_filename, 'w') as fp:
    csv_w = csv.writer(fp, delimiter='|')
    csv_w.writerows(results)

#close the connections to the files
fp.close()
m_cursor.close()
conn.close()

# load the aws_boto_credentials values
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

#access s3
s3 = boto3.client('s3',
                  aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key)


s3_file = extracted_filename

#send the csv file to the s3 bucket
s3.upload_file(extracted_filename, bucket_name, s3_file)

#we've successfully extracted the content from the DB 
#performed ingestion into an s3 bucket
