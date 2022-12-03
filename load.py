import psycopg2
import configparser
from minio import Minio
import pandas as pd

parser = configparser.ConfigParser()
parser.read("/home/maxime/elt_project/pipeline.conf")
hostname = parser.get("postgres_config", "hostname")
database = parser.get("postgres_config", "database")
username = parser.get("postgres_config", "username")
password = parser.get("postgres_config", "password")

conn = psycopg2.connect(
    host = hostname,
    database = database,
    user = username,
    password = password
)

cur = conn.cursor()

## create table
cur.execute("DROP TABLE IF EXISTS sales_data;")
cur.execute("""
    CREATE TABLE sales_data (
        id integer,
        product VARCHAR(26),
        quantity integer,
        price numeric,
        date timestamp,
        address VARCHAR(42)
    )
""")

m_hostname = parser.get("minio_config", "hostname")
access_key = parser.get("minio_config", "access_key")
secret_key = parser.get("minio_config", "secret_key")

client = Minio(
    f"{m_hostname}:9000",
    access_key = access_key,
    secret_key = secret_key,
    secure = False
)

obj = client.get_object(
    "salesbucket",
    "sales_extract.csv",
)

df = pd.read_csv(obj, header=None, delimiter='|')

for i in df.index:
    cols = "id,product,quantity,price,date,address"
    vals = [df.at[i, col] for col in range(0,6)]
    query = f"INSERT INTO sales_data({cols}) VALUES({vals[0]},'{vals[1]}',{vals[2]},{vals[3]},'{vals[4]}','{vals[5]}')"
    cur.execute(query)
conn.commit()

conn.close()