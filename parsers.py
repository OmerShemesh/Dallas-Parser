import threading
import json
import os


def parse_json_file(filename):
    with open(os.path.join(os.getcwd(), 'data', filename)) as file:
        data = json.load(file)
        return data



class DataCenterParser(threading.Thread):
    def __init__(self, cursor):
        super().__init__()
        self.__cursor = cursor
        self.__datacenters_db_list = []
        self.__datacenter_list = []

    def run(self):
        self.__cursor.execute("SELECT * FROM storage_pool")
        self.__datacenters_db_list = self.__cursor.fetchall()
        for datacenter in self.__datacenters_db_list:

            datacenter_dict = {
                '_id': datacenter['id'],
                'datacenter_name': datacenter['name'],
                'ovirt_compatibility_version': datacenter['compatibility_version'],

            }
            self.__datacenter_list.append(datacenter_dict)

    @property
    def datacenters(self):
        return self.__datacenter_list


class ClusterParser(threading.Thread):
    def __init__(self, cursor):
        super().__init__()
        self.__clusters_list = []
        self.__cursor = cursor
        self.__clusters_db_list = []
        self.__datacenters = {}

    def run(self):
        self.__cursor.execute("SELECT * FROM cluster_view")
        self.__clusters_db_list = self.__cursor.fetchall()
        for cluster in self.__clusters_db_list:
            self.__datacenters[cluster['storage_pool_id']] = self.__datacenters.get(cluster['storage_pool_id'], 0) + 1

            cluster_dict = {
                '_id': cluster['cluster_id'],
                'cluster_name': cluster['name'],
                'cpu_family': cluster['cpu_name'],
                'ovirt_compatibility_version': cluster['compatibility_version'],
                'datacenter_id': cluster['storage_pool_id'],
                'vms_count': 0,
                'hosts_count': 0
            }
            self.__clusters_list.append(cluster_dict)

    @property
    def clusters(self):
        return self.__clusters_list

    def get_datacenter_clusters_count(self,datacenter_id):
        return self.__datacenters.get(datacenter_id, 0)


class HostParser(threading.Thread):
    def __init__(self, cursor):
        super().__init__()
        self.__hosts_list = []
        self.__cursor = cursor
        self.__clusters = {}
        self.__vds_db_list = []

    def _parse_cpu_manufacturer(self, manufacturer):
        manufacturer = manufacturer.lower()

        if manufacturer.startswith('intel'):
            return 'Intel'
        elif manufacturer.startswith('amd'):
            return 'AMD'
        elif manufacturer.startswith('power'):
            return 'IBM'

    def run(self):

        self.__cursor.execute("SELECT * FROM vds")
        self.__vds_db_list = self.__cursor.fetchall()
        for host in self.__vds_db_list:
            cpu_manufacturer = self._parse_cpu_manufacturer(host['cpu_model'])

            self.__clusters[host['cluster_id']] = self.__clusters.get(host['cluster_id'], 0) + 1

            host_dict = {
                '_id': host['vds_id'],
                'cpu_model': host['cpu_model'],
                'cpu_manufacturer': cpu_manufacturer,
                'host_name': host['vds_name'],
                'host_ip': host['host_name'],
                'mem_size': host['physical_mem_mb'],
                'cpu_usage': host['usage_cpu_percent'],
                'mem_usage': host['usage_mem_percent'],
                'running_vms_count': host['vm_active'],
                'cluster_id': host['cluster_id'],
                'cpu_cores': host['cpu_cores']
            }
            self.__hosts_list.append(host_dict)

    @property
    def hosts(self):
        return self.__hosts_list

    def get_cluster_hosts_count(self, cluster_id):
        return self.__clusters.get(cluster_id, 0)


class TemplateParser(threading.Thread):
    def __init__(self, cursor):
        super().__init__()
        self.__templates_list = []
        self.__cursor = cursor
        self.__templates_db_list = []

    def run(self):
        self.__cursor.execute("SELECT * FROM vm_templates_view")
        self.__templates_db_list = self.__cursor.fetchall()
        for template in self.__templates_db_list:
            if template['name'] != "Blank":
                template_dict = {
                    '_id': template['vmt_guid'],
                    'template_name': template['name'],
                    'cluster_id': template['cluster_id'],
                    'datacenter_id': template['storage_pool_id']
                }
                self.__templates_list.append(template_dict)

    @property
    def templates(self):
        return self.__templates_list


class VirtualMachineParser(threading.Thread):
    def __init__(self, cursor):
        super().__init__()
        self.__vms_list = []
        self.__clusters = {}
        self.__running_hosts = {}
        self.__cursor = cursor
        self.__vms_db_list = []

    def run(self):
        self.__cursor.execute("SELECT * FROM vms")
        self.__vms_db_list = self.__cursor.fetchall()

        for vm in self.__vms_db_list:
            self.__clusters[vm['cluster_id']] = self.__clusters.get(vm['cluster_id'], 0) + 1
            self.__running_hosts[vm['run_on_vds']] = self.__running_hosts.get(vm['run_on_vds'], 0) + 1

            os_types = parse_json_file('os_types.json')
            if str(vm['os']) in os_types:
                vm_dict = {
                    '_id': vm['vm_guid'],
                    'vm_name': vm['vm_name'],
                    'mem_size': vm['mem_size_mb'],
                    'cluster_id': vm['cluster_id'],
                    'running_host': vm['run_on_vds'],
                    'os_type': os_types[str(vm['os'])],
                }

                self.__vms_list.append(vm_dict)

    def get_cluster_vm_count(self, cluster_id):
        return self.__clusters.get(cluster_id, 0)

    def get_host_running_vm_count(self, host_id):
        return self.__running_hosts.get(host_id, 0)

    @property
    def vms(self):
        return self.__vms_list


class StorageParser(threading.Thread):

    def __init__(self, cursor):
        super().__init__()
        self.__storage_list = []
        self.__storage_db_list = []
        self.__cursor = cursor

    def run(self):
        self.__cursor.execute("SELECT * FROM storage_domains")
        self.__storage_db_list = self.__cursor.fetchall()
        storage_types = parse_json_file('storage_types.json')
        for storage in self.__storage_db_list:
            if str(storage['storage_type']) in storage_types:
                storage_dict = {
                    '_id': storage['id'],
                    'storage_type': storage_types[str(storage['storage_type'])],
                    'datacenter_id': storage['storage_pool_id']
                }
                self.__storage_list.append(storage_dict)


    @property
    def get_storage(self):
        return self.__storage_list







