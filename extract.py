import pymysql
import csv
import configparser

from minio import Minio

parser = configparser.ConfigParser()
parser.read("/home/maxime/elt_project/pipeline.conf")
hostname = parser.get("mysql_config", "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config", "username")
password = parser.get("mysql_config", "password")
dbname = parser.get("mysql_config", "database")

conn = pymysql.connect(host=hostname,
        user=username,
        password=password,
        db=dbname,
        port=int(port))

if conn is None:
    print("Error connecting to the MySQL databse")
else:
    print("MySQL connection established !")

m_query = "SELECT * FROM sales_data;"
local_filename = "sales_extract.csv"

m_cursor = conn.cursor()
m_cursor.execute(m_query)
results = m_cursor.fetchall()

with open(f"/tmp/{local_filename}", 'w') as fp:
    csv_w = csv.writer(fp, delimiter='|')
    csv_w.writerows(results)

fp.close()
m_cursor.close()
conn.close()

m_hostname = parser.get("minio_config", "hostname")
access_key = parser.get("minio_config", "access_key")
secret_key = parser.get("minio_config", "secret_key")

client = Minio(
    f"{m_hostname}:9000",
    access_key = access_key,
    secret_key = secret_key,
    secure = False
)

found = client.bucket_exists("salesbucket")
if not found:
    client.make_bucket("salesbucket")
else:
    print("Bucket 'salesbucket' already exists")

client.fput_object(
    "salesbucket", local_filename, f"/tmp/{local_filename}"
)

print(f"'{local_filename}' is successfully uploaded to bucket 'salesbucket'.")