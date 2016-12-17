import psycopg2
import psycopg2.extras

conn = psycopg2.connect("dbname=lago user=omer")

cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

cursor.execute('SELECT * FROM vds')


vds_dict_list = cursor.fetchall()

for vds_dict in vds_dict_list:
    print(vds_dict['cpu_model'])



cursor.close()
conn.close()