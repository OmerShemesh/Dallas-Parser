class DataCenterParser:
    def __init__(self, cursor):
        self.cursor = cursor
        self.cursor.execute("SELECT * FROM cluster_view")
        self.datacenters_db_list = self.cursor.fetchall()
        self.datacenter_dict = {
            '_id': self.datacenters_db_list[0]['storage_pool_id'],
            'datacenter_name': self.datacenters_db_list[0]['storage_pool_name']
        }


class ClusterParser:
    def __init__(self, cursor):
        self.clusters_list = []
        self.cursor = cursor
        self.cursor.execute("SELECT * FROM cluster_view")
        self.clusters_db_list = self.cursor.fetchall()
        for cluster in self.clusters_db_list:
            cluster_dict = {
                '_id': cluster['cluster_id'],
                'cluster_name': cluster['name'],
                'ovirt_compatibility_version': cluster['compatibility_version'],
                'datacenter_id': cluster['storage_pool_id'],
                'vms_count': 0,
                'hosts_count': 0
            }
            self.clusters_list.append(cluster_dict)


class HostParser:
    def __init__(self, cursor):
        self.hosts_list = []
        self.cursor = cursor
        self.clusters = {}
        self.cursor.execute("SELECT * FROM vds")
        self.vds_db_list = self.cursor.fetchall()
        for vds_host in self.vds_db_list:
            cpu_manufacturer = ""
            if vds_host['cpu_model'].startswith("Intel"):
                cpu_manufacturer = "Intel"
            elif vds_host['cpu_model'].startswith("AMD"):
                cpu_manufacturer = "AMD"

            self.clusters[vds_host['cluster_id']] = self.clusters.get(vds_host['cluster_id'], 0) + 1

            host_dict = {
                '_id': vds_host['vds_id'],
                'cpu_model': vds_host['cpu_model'],
                'cpu_manufacturer': cpu_manufacturer,
                'host_name': vds_host['vds_name'],
                'host_ip': vds_host['host_name'],
                'mem_size': vds_host['physical_mem_mb'],
                'cpu_usage': vds_host['usage_cpu_percent'],
                'mem_usage': vds_host['usage_mem_percent'],
                'running_vms_count': 0,
                'cluster_id': vds_host['cluster_id']
            }
            self.hosts_list.append(host_dict)

    def get_cluster_hosts_count(self, cluster_id):
        return self.clusters[cluster_id]


class TemplateParser:
    def __init__(self, cursor):
        self.templates_list = []
        self.cursor = cursor
        self.cursor.execute("SELECT * FROM vm_templates_view")
        self.templates_db_list = self.cursor.fetchall()
        for template in self.templates_db_list:
            if template['name'] != "Blank":
                template_dict = {
                    '_id': template['vmt_guid'],
                    'template_name': template['name'],
                    'cluster_id': template['cluster_id'],
                    'datacenter_id': template['storage_pool_id']
                }
                self.templates_list.append(template_dict)


class VirtualMachineParser:
    def __init__(self, cursor):
        self.vms_list = []
        self.clusters = {}
        self.running_hosts = {}
        self.cursor = cursor
        self.cursor.execute("SELECT * FROM vms")
        self.vms_db_list = self.cursor.fetchall()

        for vm in self.vms_db_list:
            self.clusters[vm['cluster_id']] = self.clusters.get(vm['cluster_id'], 0) + 1
            self.running_hosts[vm['run_on_vds']] = self.running_hosts.get(vm['run_on_vds'], 0) + 1
            vm_dict = {
                '_id': vm['vm_guid'],
                'vm_name': vm['vm_name'],
                'mem_size': vm['mem_size_mb'],
                'cluster_id': vm['cluster_id'],
                'running_host': vm['run_on_vds']
            }

            self.vms_list.append(vm_dict)

    def get_cluster_vm_count(self, cluster_id):
        return self.clusters[cluster_id]

    def get_host_running_vm_count(self, host_id):
        return self.running_hosts.get(host_id,0)