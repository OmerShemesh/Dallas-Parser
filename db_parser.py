import psycopg2
import psycopg2.extras

from parsers import *

conn = psycopg2.connect("dbname=lago user=omer")

cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

general_info_parser = GeneralInfoParser(cursor)

print(general_info_parser.parse_hosts())
print(general_info_parser.parse_cluster())

conn.close()
cursor.close()