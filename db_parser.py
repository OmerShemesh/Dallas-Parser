import sys
from psycopg2 import connect, extras, OperationalError
from parsers import *
from pymongo import MongoClient, errors

postgres_dbname = sys.argv[1]
postgres_user = sys.argv[2]
setup_id = sys.argv[3]


try:
    conn = connect("dbname=%s user=%s" % (postgres_dbname, postgres_user))
    cursor_dict = {
        'dc_cursor': conn.cursor(cursor_factory=extras.DictCursor),
        'cluster_cursor': conn.cursor(cursor_factory=extras.DictCursor),
        'template_cursor': conn.cursor(cursor_factory=extras.DictCursor),
        'host_cursor': conn.cursor(cursor_factory=extras.DictCursor),
        'vm_cursor': conn.cursor(cursor_factory=extras.DictCursor),
        'storage_cursor': conn.cursor(cursor_factory=extras.DictCursor),
        'network_cursor': conn.cursor(cursor_factory=extras.DictCursor),
        'interface_cursor': conn.cursor(cursor_factory=extras.DictCursor),
        'disk_cursor': conn.cursor(cursor_factory=extras.DictCursor)
    }

    mongo_client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=10000)

except (errors.ConnectionFailure, OperationalError) as e:
    raise e

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
    storage_parser = StorageParser(cursor_dict['storage_cursor'])
    threads.append(storage_parser)
    network_parser = NetworkParser(cursor_dict['network_cursor'])
    threads.append(network_parser)
    interface_parser = NetworkInterfaceParser(cursor_dict['interface_cursor'])
    threads.append(interface_parser)
    disk_parser = DiskParser(cursor_dict['disk_cursor'])
    threads.append(disk_parser)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    db = mongo_client.dallas

    setup_collection = db.setup
    datacenter_collection = db.datacenter
    cluster_collection = db.cluster
    template_collection = db.template
    host_collection = db.host
    vm_collection = db.vm
    storage_collection = db.storage
    network_collection = db.network
    network_interface_collection = db.network_interface

    setup_collection.update({'_id': setup_id},
                            {'_id': setup_id,
                             'dcs_count': len(dc_parser.datacenters),
                             'clusters_count': len(cluster_parser.clusters),
                             'hosts_count': len(host_parser.hosts),
                             'vms_count': len(vm_parser.vms)},
                            upsert=True)

    for datacenter in dc_parser.datacenters:
        datacenter['clusters_count'] = cluster_parser.get_datacenter_clusters_count(datacenter['_id'])
        datacenter['storage_count'] = storage_parser.get_datacenter_storage_count(datacenter['_id'])
        datacenter['networks_count'] = network_parser.get_datacenter_networks_count(datacenter['_id'])
        datacenter['setup_id'] = setup_id
        datacenter_collection.update({'_id': datacenter['_id']}, datacenter, upsert=True)

    for cluster in cluster_parser.clusters:
        cluster['hosts_count'] = host_parser.get_cluster_hosts_count(cluster['_id'])
        cluster['vms_count'] = vm_parser.get_cluster_vm_count(cluster['_id'])
        cluster_collection.update({'_id': cluster['_id']}, cluster, upsert=True)

    for template in template_parser.templates:
        template_collection.update({'_id': template['_id']}, template, upsert=True)

    for host in host_parser.hosts:
        # host['running_vms_count'] = vm_parser.get_host_running_vm_count(host['_id'])
        host['nics_count'] = interface_parser.get_host_interfaces_count(host['_id'])
        host_collection.update({'_id': host['_id']}, host, upsert=True)

    for vm in vm_parser.vms:
        vm['nics_count'] = interface_parser.get_vm_interfaces_count(vm['_id'])
        vm['disks_count'] = disk_parser.get_vm_disks_count(vm['_id'])
        vm_collection.update({'_id': vm['_id']}, vm, upsert=True)

    for storage in storage_parser.get_storage:
        storage_collection.update({'_id': storage['_id']}, storage, upsert=True)

    for network in network_parser.networks:
        network_collection.update({'_id': network['_id']}, network, upsert=True)

    for interface in interface_parser.interfaces:
        network_interface_collection.update({'_id': interface['_id']}, interface, upsert=True)

    for cursor in cursor_dict.values():
        cursor.close()

    conn.close()
    mongo_client.close()
