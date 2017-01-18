import sys
from psycopg2 import connect, extras, OperationalError
from parsers import *
from pymongo import MongoClient, errors

postgres_dbname = sys.argv[1]
postgres_user = sys.argv[2]

try:
    conn = connect("dbname=%s user=%s" % (postgres_dbname, postgres_user))
    cursor_dict = {
        'dc_cursor': conn.cursor(cursor_factory=extras.DictCursor),
        'cluster_cursor': conn.cursor(cursor_factory=extras.DictCursor),
        'template_cursor': conn.cursor(cursor_factory=extras.DictCursor),
        'host_cursor': conn.cursor(cursor_factory=extras.DictCursor),
        'vm_cursor': conn.cursor(cursor_factory=extras.DictCursor)
    }

    mongo_client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=10000)

except (errors.ConnectionFailure, OperationalError) as e:
    print(e)

else:
    threads = []

    dc_parser = DataCenterParser(cursor_dict['dc_cursor'])
    threads.append(dc_parser)
    cluster_parser = ClusterParser(cursor_dict['cluster_cursor'])
    threads.append(cluster_parser)
    template_parser = TemplateParser(cursor_dict['template_cursor'])
    threads.append(template_parser)
    host_parser = HostParser(cursor_dict['host_cursor'])
    threads.append(host_parser)
    vm_parser = VirtualMachineParser(cursor_dict['vm_cursor'])
    threads.append(vm_parser)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    db = mongo_client.dallas

    datacenter_collection = db.datacenter
    cluster_collection = db.cluster
    template_collection = db.template
    host_collection = db.host
    vm_collection = db.vm

    datacenter_collection.insert_one(dc_parser.datacenter_dict)

    for cluster in cluster_parser.clusters_list:
        cluster['hosts_count'] = host_parser.get_cluster_hosts_count(cluster['_id'])
        cluster['vms_count'] = vm_parser.get_cluster_vm_count(cluster['_id'])
        cluster_collection.insert_one(cluster)

    for template in template_parser.templates_list:
        template_collection.insert_one(template)

    for host in host_parser.hosts_list:
        host['running_vms_count'] = vm_parser.get_host_running_vm_count(host['_id'])
        host_collection.insert_one(host)

    for vm in vm_parser.vms_list:
        vm_collection.insert_one(vm)

    for cursor in cursor_dict.values():
        cursor.close()
    conn.close()
    mongo_client.close()