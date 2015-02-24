__author__ = "Gopi Raghavan"
__copyright__ = "Copyright (C) 2015 Gopi Raghavan"
__license__ = "GPL"
__version__ = "1.0"

from timeit import default_timer as timer
import json
import random
import decimal
import string
import datetime
from datetime import date
import multiprocessing as mp

# Define an output queue
output = mp.Queue()
cpu_count = mp.cpu_count()

def generate_random_dates(start_date, end_date):
	start = start_date.toordinal()
	end = end_date.toordinal()
	random_value = random.randint(start, end)
	random_date = date.fromordinal(random_value)
	return random_date

def generate_random_timestamp(start_ts, end_ts):
	rnd_year=end_ts.year
	rnd_mth=end_ts.month
	rnd_day=end_ts.day
	rnd_hour=end_ts.hour
	rnd_mins=end_ts.minute
	rnd_secs=end_ts.second
	if (start_ts.year < end_ts.year):
		rnd_year=random.choice(range(start_ts.year, end_ts.year))
	if (start_ts.month < end_ts.month):
		rnd_mth=random.choice(range(start_ts.month, end_ts.month))
	if (start_ts.day < end_ts.day):
		rnd_day=random.choice(range(start_ts.day, end_ts.day))
	if (start_ts.hour < end_ts.hour):
		rnd_hour=random.choice(range(start_ts.hour, end_ts.hour))
	if (start_ts.minute < end_ts.minute):
		rnd_mins=random.choice(range(start_ts.minute, end_ts.minute))
	if (start_ts.second < end_ts.second):
		rnd_secs=random.choice(range(start_ts.second, end_ts.second))
	return datetime.datetime(rnd_year, rnd_mth, rnd_day, rnd_hour, rnd_mins, rnd_secs)

sample_file_name = ""
output_file_name = ""
delimiter = ""
max_rows_per_file = 0
generate_from_sample = "N"
gen_based_on_row_def = "N"
fin = None
fout = None

class Column(object):
	field_position = 0
	field_name = ""
	field_type = ""
	values = {}

	def __init__(self, field_name, field_type, field_position, values):
		self.field_name = field_name
		self.field_type = field_type
		self.field_position = field_position
		self.values = values

	def __repr__(self):
		return '\n {}: {} | {} | {} | {} | {}'.format(self.__class__.__name__, self.field_name
			,self.field_type,self.field_position,self.values['MIN'], self.values['MAX'])

	def __cmp__(self, other):
		if hasattr(other, 'field_position'):
			return self.field_position.__cmp__(other.field_position)

row_def = []
auto_num_inc_field_pos = [0]

data_config=open('data_config.json')

data = json.load(data_config)

for file_info in data['file_info']['config']:
	generate_from_sample = file_info['generate_from_sample']
	gen_based_on_row_def = file_info['gen_based_on_row_def']
	
	if generate_from_sample == 'Y' :
		sample_file_name = file_info['sample_file_name']
		print 'Generating data from sample input file: ' + sample_file_name

	output_file_name = file_info['output_file_name']
	delimiter = file_info['delimiter']
	max_rows_per_file = int(file_info['max_rows_per_file'])
	auto_num_inc_field_pos= file_info['auto_num_inc_field_pos']

if gen_based_on_row_def is not None:
	for columns in data['row']['columns']:
	 	
	 	for val in columns['values']:
	 		vals = {} # this is because the dict object is mutable
	 		vals['MIN'] = val['MIN']
	 		vals['MAX'] = val['MAX']

		col = Column(columns['field_name'], columns['field_type']
				, columns['field_position'], vals)
		row_def.append(col)

data_config.close()

if generate_from_sample == 'Y':
	fin = open(sample_file_name)

if output_file_name is not None:
	fout = open(output_file_name, "w")
else:
	print 'no output file name provided.. aborting...'
	raise SystemExit

row_def = sorted(row_def)

maxcol = len(row_def) - 1
mincol = 0

rows = []

def generateRows(row_def, file_name, process_num):
	row=""
	outfile = open(file_name + str(process_num) + '.csv', "w")
	for i in range (0, max_rows_per_file):
		outfile.write(row + '\n')
		for col_def in row_def:
			if col_def.field_position == mincol:
				row = ""
			if col_def.field_type == 'INT':
				row=row+str(random.randrange(col_def.values['MIN'],col_def.values['MAX']))
			elif col_def.field_type == 'DECIMAL':
				row=row+str(decimal.Decimal('%d.%d' % (random.randint(col_def.values['MIN'], col_def.values['MAX'])
					,random.randint(col_def.values['MIN'],col_def.values['MAX']))))
			elif col_def.field_type == 'STRING':
				row = row + ''.join(random.choice(
                    string.ascii_lowercase
                    + string.ascii_uppercase
                    + string.digits)
                for i in range(col_def.values['MAX']))
			elif col_def.field_type == 'DATE':
				random_date = generate_random_dates(
					datetime.datetime.strptime(col_def.values['MIN'], "%Y-%m-%d").date()
					, datetime.datetime.strptime(col_def.values['MAX'], "%Y-%m-%d").date())
				row = row + random_date.strftime("%Y-%m-%d")
			elif col_def.field_type == 'TIMESTAMP':
				random_ts = generate_random_timestamp(
					datetime.datetime.strptime(col_def.values['MIN'], "%Y-%m-%d %H:%M:%S")
					, datetime.datetime.strptime(col_def.values['MAX'], "%Y-%m-%d %H:%M:%S"))
				row = row + random_ts.strftime("%Y-%m-%d %H:%M:%S")
			else:
				pass
			if col_def.field_position < maxcol:
				row = row + delimiter
			#print i
	outfile.close()

if gen_based_on_row_def == 'Y':
	print 'Generating data based on row definition'
	print 'please verify the following configuration'
	print row_def
	strt_time = timer()
	# Setup a list of processes that we want to run
	processes = [mp.Process(target=generateRows, args=(row_def, "output", str(x))) for x in range(cpu_count)]

	# Run processes
	for p in processes:
	    p.start()

	# Exit the completed processes
	for p in processes:
	    p.join()

	end_time = timer()
	print 'completed generating data in : ' + str(end_time - strt_time) + ' seconds'

#This function is to generate data from a sample file with increments on a field position
def genNumIncRecords(tokens, auto_num_inc_field_pos, delimiter, out_file):
	for index in range(len(tokens)):
		if (index == auto_num_inc_field_pos):
			out_file.write(delimiter.join(tokens))
			for i in range (0, max_rows_per_file) :
				new_value = int(tokens[index]) + 1
				tokens[index]=str(new_value)
				out_file.write(delimiter.join(tokens))

if fin is not None:
	print "Reading sample file " + sample_file_name
	out_file_name = sample_file_name+'.out.csv'
	out_file=open(out_file_name, "w")
	for line in fin:
		tokens=line.split(delimiter)
		genNumIncRecords(tokens, auto_num_inc_field_pos, delimiter, out_file)
	out_file.close()

try:
	if fin is not None:
		fin.close()

	if fout is not None:
		fout.close()
except Exception, e:
	print 'unable to close file handlers'
finally:
	print 'completed generating test data'
	print 'thanks for trying... ' + __copyright__

