# tdm
Test data generator using python. 

1. data_generator.py 		--> TDM can generate test data based on a configuration file i.e. data_config.json.
	1.1. Allows for any number of columns
	1.2. Allows for various data types
	1.3. Allows for configuring data generation within a range of values
	1.4. Allows for configuring data generation within a given set of values
	
2. generate_from_ddl.py --> TDM can connect to a database and discover the table description then use that to generate test data. Configure your database connection details in gen_from_ddl_config.json. Once the database table description is read from the database, this program will generate an output.json which can then be modified to run with #1 above. 

This uses pyodbc to connect to the database.

