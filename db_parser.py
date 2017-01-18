import sys
import threading
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
    parsers = {}

    def get_parser(parser_name):
        if parser_name == 'dc_parser':
            parsers[parser_name] = DataCenterParser(cursor_dict['dc_cursor'])
        if parser_name == 'cluster_parser':
            parsers[parser_name] = ClusterParser(cursor_dict['cluster_cursor'])
        if parser_name == 'template_parser':
            parsers[parser_name] = TemplateParser(cursor_dict['template_cursor'])
        if parser_name == 'host_parser':
            parsers[parser_name] = HostParser(cursor_dict['host_cursor'])
        if parser_name == 'vm_parser':
            parsers[parser_name] = VirtualMachineParser(cursor_dict['vm_cursor'])

    threads = []
    dc_thread = threading.Thread(target=get_parser, args=('dc_parser',))
    threads.append(dc_thread)
    cluster_thread = threading.Thread(target=get_parser, args=('cluster_parser',))
    threads.append(cluster_thread)
    template_thread = threading.Thread(target=get_parser, args=('template_parser',))
    threads.append(template_thread)
    host_thread = threading.Thread(target=get_parser, args=('host_parser',))
    threads.append(host_thread)
    vm_thread = threading.Thread(target=get_parser, args=('vm_parser',))
    threads.append(vm_thread)

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

    datacenter_collection.insert_one(parsers['dc_parser'].datacenter_dict)

    for cluster in parsers['cluster_parser'].clusters_list:
        cluster['hosts_count'] = parsers['host_parser'].get_cluster_hosts_count(cluster['_id'])
        cluster['vms_count'] = parsers['vm_parser'].get_cluster_vm_count(cluster['_id'])
        cluster_collection.insert_one(cluster)

    for template in parsers['template_parser'].templates_list:
        template_collection.insert_one(template)

    for host in parsers['host_parser'].hosts_list:
        host['running_vms_count'] = parsers['vm_parser'].get_host_running_vm_count(host['_id'])
        host_collection.insert_one(host)

    for vm in parsers['vm_parser'].vms_list:
        vm_collection.insert_one(vm)

    conn.close()
