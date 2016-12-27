
class GeneralInfoParser:
    def __init__(self, cursor):
        self.cursor = cursor
        self.cursor.execute('SELECT * FROM vds')
        self.vds_list = self.cursor.fetchall()

    def parse_cluster(self):

        cluster_dict = {'_id': self.vds_list[0]['cluster_id'], 'cluster_name': self.vds_list[0]['cluster_name'],
                        'hosts': []}

        for vds_host in self.vds_list:
            cluster_dict['hosts'].append(vds_host['vds_id'])
        return cluster_dict

    def parse_hosts(self):
        hosts_list = []
        for vds_host in self.vds_list:
            hosts_list.append(
                {
                    '_id': vds_host['vds_id'],
                    'cpu_model': vds_host['cpu_model'],
                    'host_name': vds_host['vds_name'],
                    'host_ip': vds_host['host_name']
                })

        return hosts_list
