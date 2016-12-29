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

    def parse(self):
        datacenter_list = []
        for datacenter in self.clusters_db_list:

            datacenter_dict = {
                '_id': self.clusters_db_list[0]['storage_pool_id'],
                'datacenter_name': self.clusters_db_list[0]['storage_pool_name'],
                'clusters': self.__parse_cluster(datacenter['storage_pool_id']),
                'templates': self.__parse_template(datacenter['storage_pool_id'])
            }

            datacenter_list.append(datacenter_dict)

        return datacenter_list

    def __parse_template(self,datacenter_id):
        templates_list = []
        for template in self.templates_db_list:
            if template['name'] != "Blank" and template['storage_pool_id'] == datacenter_id:
                template_dict = {
                    '_id': template['vmt_guid'],
                    'template_name': template['name'],
                    'cluster_id': template['cluster_id']
                }
                templates_list.append(template_dict)
        return templates_list

    def __parse_cluster(self,datacenter_id):

        clusters_list = []
        for cluster in self.clusters_db_list:
            if cluster['storage_pool_id'] == datacenter_id:
                cluster_dict = {
                    '_id': cluster['cluster_id'],
                    'cluster_name': cluster['name'],
                    'ovirt_compatibility_version': cluster['compatibility_version'],
                    'hosts': self.__parse_hosts(cluster['cluster_id']),
                    'vms': self.__parse_vms(cluster_id=cluster['cluster_id'])}

            # for vds_host in self.vds_db_list:
            #     cluster_dict['hosts'].append(vds_host['vds_id'])
            #
            # for vm in self.vms_db_list:
            #     if vm['cluster_id'] == cluster_dict['_id']:
            #         cluster_dict['vms'].append(vm['vm_guid'])

                clusters_list.append(cluster_dict)

            return clusters_list

    def __parse_hosts(self,cluster_id):
        hosts_list = []
        for vds_host in self.vds_db_list:
            if vds_host['cluster_id'] == cluster_id:
                host_dict = {
                    '_id': vds_host['vds_id'],
                    'cpu_model': vds_host['cpu_model'],
                    'host_os': vds_host['host_os'],
                    'host_name': vds_host['vds_name'],
                    'host_ip': vds_host['host_name'],
                    'mem_size': vds_host['physical_mem_mb'],
                    'cpu_usage': vds_host['usage_cpu_percent'],
                    'mem_usage': vds_host['usage_mem_percent'],
                    'running_vms': self.__parse_vms(running_host_id=vds_host['vds_id'])

                }

            # for vm in self.vms_db_list:
            #     if vm['run_on_vds'] == host_dict['_id']:
            #         host_dict['running_vms'].append(vm['vm_guid'])

                hosts_list.append(host_dict)

        return hosts_list

    def __parse_vms(self,running_host_id=None,cluster_id=None):

        vms_list = []

        for vm in self.vms_db_list:
            if running_host_id is not None and cluster_id is None:
                if vm['run_on_vds'] == running_host_id:
                    vms_list.append(
                        {
                            '_id': vm['vm_guid'],
                            'vm_name': vm['vm_name'],
                            'mem_size': vm['mem_size_mb'],
                        })
            elif cluster_id is not None and running_host_id is None:
                if vm['cluster_id'] == cluster_id:
                    vms_list.append(
                        {
                            '_id': vm['vm_guid'],
                            'vm_name': vm['vm_name'],
                            'mem_size': vm['mem_size_mb'],
                        })

        return vms_list
