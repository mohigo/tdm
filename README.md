## tdm

Test data generator using python. Use this to generate test data for testing. Can be used to generate mock data for functional testing or can generate data at scale for performance testing. 

## How to generate data from a config? _(data_generator.py)_

TDM can generate test data based on a configuration file i.e. _data_config.json_
- Allows for any number of columns
- Allows for various data types
- Allows for configuring data generation within a range of values
- Allows for configuring data generation within a given set of values

	
## Discover database table definition to generate test data? _(generate_from_ddl.py)_

TDM can connect to a database and discover the table description then use that to generate test data. Configure your database connection details in _gen_from_ddl_config.json_. Once the database table description is read from the database, this program will generate an _output.json_ which can then be modified to run with #1 above. 

This uses pyodbc to connect to the database.
