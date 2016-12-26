
import psycopg2
import psycopg2.extras

class GeneralInfoParser:

    def __init__(self, cursor):
        self.cursor = cursor

    def parse(self):
        self.cursor.execute('SELECT * FROM vds')

        vds_host_list = self.cursor.fetchall()



        hosts_list = []
        for vds_host in vds_host_list:
            hosts_list.append({'cpu_model': vds_host['cpu_model'], 'host_name': vds_host['vds_name']})

        return hosts_list





