import psycopg2
from psycopg2 import extras
from parsers import *
from pymongo import MongoClient

conn = psycopg2.connect("dbname=lago user=omer")
cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

mongo_client = MongoClient('mongodb://localhost:27017/')

db = mongo_client.dallas

datacenter_parser = DataCenterParser(cursor)
cluster_parser = ClusterParser(cursor)
template_parser = TemplateParser(cursor)
host_parser = HostParser(cursor)
vm_parser = VirtualMachineParser(cursor)


datacenter_collection = db.datacenter
cluster_collection = db.cluster
template_collection = db.template
host_collection = db.host
vm_collection = db.vm


datacenter_collection.insert_one(datacenter_parser.datacenter_dict)

for cluster in cluster_parser.clusters_list:
    cluster_collection.insert_one(cluster)

for template in template_parser.templates_list:
    template_collection.insert_one(template)

for host in host_parser.hosts_list:
    host_collection.insert_one(host)

for vm in vm_parser.vms_list:
    vm_collection.insert_one(vm)

conn.close()
cursor.close()


