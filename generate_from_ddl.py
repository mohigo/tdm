import pyodbc
import sys
import re
import getpass
import json

database_name=''
table_name=''

json_data=open('gen_from_ddl_config.json')

data = json.load(json_data)

for extract_ddl in data['extract_ddl']['config']:
	dsn = extract_ddl['dsn']
	database_name = str(extract_ddl['database_name'])
	table_name = str(extract_ddl['table_name'])
	output_file_name = extract_ddl['output_file_name']
	
json_data.close()

pw = getpass.getpass()
user = getpass.getuser()

pyodbc.pooling=False
conn_string = 'DSN=' + dsn + ';PORT=1025;UID=' + user + ';PWD=' + pw + ';'
conn = pyodbc.connect(conn_string)
cursor = conn.cursor()

query = 'select columnname, columntype, '
query = query + 'CASE WHEN CharType <> 0 THEN columnlength '
query = query + 'ELSE \'\' END AS charlength from DBC.Columns C WHERE databasename = ? '
query = query + 'and tablename = ? order by columnId;'

cursor.execute(query, database_name, table_name)
rows=cursor.fetchall()
i=0

cols=[]

for row in rows:
	print row[0], row[1], row[2]
	field_position=i
	col_name=row[0]
	col_type='UNKNOWN'
	col_min=''
	col_max=''
	type_check=row[1].strip()

	if type_check == 'AT':
		col_type='TIMESTAMP'
	elif type_check == 'BF':
		col_type='BYTE'
	elif type_check == 'BO':
		col_type='BLOB'
	elif type_check == 'BV':
		col_type='VARBYTE'
	elif type_check == 'CF':
		col_type='CHAR'
	elif type_check == 'CO':
		col_type='CLOB'
	elif type_check == 'CV':
		col_type='VARCHAR'
	elif type_check == 'D':
		col_type='DECIMAL'
	elif type_check == 'DA':
		col_type='DATE'
	elif type_check == 'F':
		col_type='DECIMAL'
	elif type_check == 'I1':
		col_type='INT'
		col_min='-128'
		col_max='127'
	elif type_check == 'I2':
		col_type='INT'
		col_min='-32768'
		col_max='32767'
	elif type_check == 'I8':
		col_type='INT'
	elif type_check == 'I':
		col_type='INT'
	elif type_check == 'SZ':
		col_type='TIMESTAMP'
	elif type_check == 'TS':
		col_type='TIMESTAMP'
	elif type_check == 'TZ':
		col_type='TIMESTAMP'
	else:
		pass

	if col_max == '':
		col_max=row[2].strip()

	c={'field_position':field_position, 'field_name':col_name.strip(), 'field_type':col_type, 'values':{'MIN':col_min,'MAX':col_max}}
	cols.append(c)
	i+=1

n={'columns' : cols}
outfile = open(output_file_name, "w")

outfile.write(json.dumps(n, indent=4, sort_keys=True))

outfile.close()
conn.close()
