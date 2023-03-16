# 3NF GENERATOR

## Objectives

- Generate functional dependencies

- Test Lossless join

- Synthesize by Lossless Join

- Synthesize by Dependency Preservation

## Usage

- Install the employees database from `test_db`
    - Change directory to `test_db`
    - Run `mysql -u root -p < employees.sql`
- Create a python virtual environment
    - `python3 -m venv venv`
    - Activate the virtual environment by `source venv/bin/activate`
    - Install the dependencies by `pip install -r requirements.txt`
- Create a custom sql user for the program
    - Run `mysql -u root -p < create_user.sql`
- Run the program
    - `python3 main.py`

## Functions

- `connect_db()`
    - Connect to the database
    - Returns the connections
- `get_number_of_rows(table_name)`
    - Get the number of rows in a table
    - Returns the number of rows
- `get_functional_dependencies(table_name)`
    - Get the functional dependencies of a table
    - Returns:
        - functional_dependencies: a dictionary of functional dependencies
        - columns: a set of columns
        - candidate_keys: a set of candidate keys
        - primary_key: the primary key
- `lossless_join_test(table_name, functional_dependencies, columms)`
    - Iterates through all the functional dependencies and checks if the table is lossless join verified
    - Returns:
        - True if the table is lossless join verified
        - False if the table is not lossless join verified
- `three_nf_synthesis_dependency_preservation(table_name, functional_dependencies, columns, candidate_keys, primary_key)`
    - Synthesize the table into 3NF by dependency preservation
    - Returns:
        - List of tables in 3NF
- `three_nf_synthesis_lossless_join(table_name, functional_dependencies, columns, candidate_keys, primary_key)`
    - Synthesize the table into 3NF by lossless join
    - Returns:
        - List of tables in 3NF