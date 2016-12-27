import psycopg2
from psycopg2 import extras
from parsers import *
from pymongo import MongoClient

conn = psycopg2.connect("dbname=lago user=omer")
cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

mongo_client = MongoClient('mongodb://localhost:27017/')

db = mongo_client.dallas

general_info_parser = GeneralInfoParser(cursor)

cluster_collection = db.cluster
host_collection = db.host


cluster_collection.insert_one(general_info_parser.parse_cluster())

for host in general_info_parser.parse_hosts():
    host_collection.insert_one(host)

conn.close()
cursor.close()