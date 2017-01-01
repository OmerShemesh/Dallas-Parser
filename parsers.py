class GeneralInfoParser:
    def __init__(self, cursor):
        self.cursor = cursor
        self.cursor.execute("SELECT * FROM vds")
        self.vds_db_list = self.cursor.fetchall()
        self.cursor.execute("SELECT * FROM vms")
        self.vms_db_list = self.cursor.fetchall()
        self.cursor.execute("SELECT * FROM cluster_view")
        self.clusters_db_list = self.cursor.fetchall()
        self.cursor.execute("SELECT * FROM vm_templates_view")
        self.templates_db_list = self.cursor.fetchall()

    def parse_template(self):
        templates_list = []
        for template in self.templates_db_list:
            if template['name'] != "Blank":
                template_dict = {
                    '_id': template['vmt_guid'],
                    'template_name': template['name'],
                    'cluster_id': template['cluster_id']
                }
                templates_list.append(template_dict)
        return templates_list

    def parse_cluster(self):

        clusters_list = []
        for cluster in self.clusters_db_list:
            cluster_dict = {
                '_id': cluster['cluster_id'],
                'cluster_name': cluster['name'],
                'ovirt_compatibility_version': cluster['compatibility_version'],
                'hosts': [],
                'vms': []}

            for vds_host in self.vds_db_list:
                cluster_dict['hosts'].append(vds_host['vds_id'])

            for vm in self.vms_db_list:
                if vm['cluster_id'] == cluster_dict['_id']:
                    cluster_dict['vms'].append(vm['vm_guid'])

            clusters_list.append(cluster_dict)

            return clusters_list

    def parse_hosts(self):
        hosts_list = []
        for vds_host in self.vds_db_list:
            host_dict = {
                '_id': vds_host['vds_id'],
                'cpu_model': vds_host['cpu_model'],
                'host_name': vds_host['vds_name'],
                'host_ip': vds_host['host_name'],
                'mem_size': vds_host['physical_mem_mb'],
                'cpu_usage': vds_host['usage_cpu_percent'],
                'mem_usage': vds_host['usage_mem_percent'],
                'running_vms': []
            }

            for vm in self.vms_db_list:
                if vm['run_on_vds'] == host_dict['_id']:
                    host_dict['running_vms'].append(vm['vm_guid'])

            hosts_list.append(host_dict)

        return hosts_list

    def parse_vms(self):

        vms_list = []
        for vm in self.vms_db_list:
            vms_list.append(
                {
                    '_id': vm['vm_guid'],
                    'vm_name': vm['vm_name'],
                    'mem_size': vm['mem_size_mb'],
                })

        return vms_list
