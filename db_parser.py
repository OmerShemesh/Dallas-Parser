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
template_collection = db.template
host_collection = db.host
vm_collection = db.vm


for cluster in general_info_parser.parse_cluster():
    cluster_collection.insert_one(cluster)

for template in general_info_parser.parse_template():
    template_collection.insert_one(template)

for host in general_info_parser.parse_hosts():
    host_collection.insert_one(host)

for vm in general_info_parser.parse_vms():
    vm_collection.insert_one(vm)

conn.close()
cursor.close()
