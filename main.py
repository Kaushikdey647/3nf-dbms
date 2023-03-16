'''
NAME: KAUSHIK DEY
ROLL: 20CS01043

'''

import mysql.connector
from tqdm import tqdm, trange

def connect_db():

    # object to hold the connection
    mydb = None

    try:

        # connection oarameters
        mydb = mysql.connector.connect(
            host = 'localhost',
            user = 'test_user',
            password = 'Aauth123',
            port = '3306',
            database = 'employees'
        )

    except Exception as e:
        print('Error: ', e)
    return mydb

def get_number_of_rows(table_name):
    conn = connect_db()
    cursor = conn.cursor()

    # get count of all the rows
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")

    # get the count by fetching the first row of the result
    number_of_rows = cursor.fetchall()[0][0]

    # close connection
    cursor.close()
    conn.close()

    return number_of_rows

def get_functional_dependencies(table_name):

    conn = connect_db()

    #Get your cursor
    cursor = conn.cursor()

    # get all columns
    cursor.execute(
        f"DESCRIBE {table_name}"
    )

    # get all columns
    columns = [col[0] for col in tqdm(cursor.fetchall(), desc='Getting columns')]

    functional_dependencies = {}
    candidate_keys = set()

    # generate all possible combinations of columns with SELECT
    
    for i in trange(len(columns), desc='Generating functional dependencies'):
        for j in range(len(columns)):
            if i == j:
                continue
            # check i -> j dependency
            cursor.execute(
                f"SELECT {columns[i]}, COUNT(DISTINCT {columns[j]}) FROM {table_name} GROUP BY {columns[i]}"
            )
            # if for every i there is only one j, then i -> j
            if all([row[1] == 1 for row in cursor.fetchall()]):
                try:
                    functional_dependencies[columns[i]].append(columns[j])
                except KeyError:
                    functional_dependencies[columns[i]] = [columns[j]]
    
    # find candidate keys
    for key in functional_dependencies.keys():
        if set(functional_dependencies[key]) == set(columns) - set([key]):
            candidate_keys.add(key)

    # find primary key
    cursor.execute(
        f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY'"
    )

    # if exists, then it is the primary key, else return None
    try:
        primary_key = cursor.fetchall()[0][4]
    except IndexError:
        primary_key = None

    # close connection
    cursor.close()
    conn.close()

    return functional_dependencies, set(columns), candidate_keys, primary_key

def lossless_join_test(table_name, functional_dependencies=None, columns=None):
    if functional_dependencies is None or columns is None:
        functional_dependencies, columns = get_functional_dependencies(table_name)

    reconstructed_table = set()

    # reconstruct the table
    for key in functional_dependencies.keys():

        # append the key
        reconstructed_table.add(key)

        # append the values
        for value in functional_dependencies[key]:
            reconstructed_table.add(value)
    
    # check if the reconstructed table is equal to the original table
    return reconstructed_table == columns


# 3NF synthesis with dependency preservation

'''

The rule says that if we decompose a table R into (R[1], R[2], R[3] ... R[n]) for each R[i]

- For every subset a of attributes of R[i] we need to check that closure(a) either includes:
    - 1: All attributes of relation R[i]
    - 2: Or includes no attributes of R[i] - a

The algorithm for the same would be as follows:

- Create a new table for each functional dependency (condition 1)
- Create a new table for the remaining columns (condition 2)
- Create a new table for the primary key
- Create a new table for the foreign keys

Input: A set of functional dependencies F.

Output: A set of relations that are in 3NF and preserve all functional dependencies in F.

- Identify the candidate keys of the relation using Armstrong's axioms.

- Determine the minimal cover of F using the algorithm for finding the minimal cover of F.

- For each functional dependency X → Y in the minimal cover of F, do the following:
    - If X is a superkey, create a new relation R with attributes X and Y. R is in 3NF and preserves X → Y.
    - If X is not a superkey, create a new relation R with attributes X and all attributes that are functionally dependent on X in the minimal cover of F. R is in 3NF and preserves all functional dependencies in F that involve X.

- If any of the new relations created in step 3(b) have FDs that violate 3NF, repeat steps 2-3 until all relations are in 3NF.

- Combine the new relations into a set of 3NF relations by linking them with foreign keys. Each foreign key references the primary key of the relation it depends on.

- Output the set of 3NF relations.

'''
def three_nf_synthesis_dependency_preservation(
    table_name,
    functional_dependencies,
    columns,
    primary_key = None
):
    tables = []
    used_keys = set()
    # create a new table for each functional dependency
    for key in functional_dependencies.keys():
        new_table = {}
        new_table['name'] = table_name + '_fd_' + key
        new_table['primary_key'] = key
        new_table['columns'] = set()
        new_table['columns'].add(key)
        used_keys.add(key)
        for value in functional_dependencies[key]:
            used_keys.add(value)
            new_table['columns'].add(value)
        tables.append(new_table)
    
    # create a new table for the remaining columns
    remaining_columns = columns - used_keys
    if len(remaining_columns) > 0:
        remaining_columns_table = {}
        remaining_columns_table['name'] = table_name + '_remaining_columns'
        remaining_columns_table['primary_key'] = None
        remaining_columns_table['columns'] = remaining_columns
        tables.append(remaining_columns_table)

    return tables

# 3NF synthesis with lossless join
def three_nf_synthesis_lossless_join(
        table_name,
        functional_dependencies,
        columns,
        primary_key = None
):
    tables = []
    used_keys = set()
    # create a new table for each functional dependency
    for key in functional_dependencies.keys():
        new_table = {}
        new_table['name'] = table_name + '_fd_' + key
        new_table['primary_key'] = key
        new_table['columns'] = set()
        new_table['columns'].add(key)
        used_keys.add(key)
        for value in functional_dependencies[key]:
            used_keys.add(value)
            new_table['columns'].add(value)
        tables.append(new_table)
    
    # create a new table for the remaining columns
    remaining_columns = columns - used_keys
    if len(remaining_columns) > 0:
        remaining_columns_table = {}
        remaining_columns_table['name'] = table_name + '_remaining_columns'
        remaining_columns_table['primary_key'] = None
        remaining_columns_table['columns'] = remaining_columns
        tables.append(remaining_columns_table)

    # create a new table for the foreign keys
    foreign_keys_table = {}
    foreign_keys_table['name'] = table_name + '_foreign_keys'
    foreign_keys_table['primary_key'] = primary_key
    foreign_keys_table['columns'] = set()
    for table in tables:
        if table['primary_key'] == primary_key:
            continue
        foreign_keys_table['columns'].add(table['primary_key'])

    return tables
    

if __name__ == '__main__':
    table_name = 'employees'
    row_count = get_number_of_rows(table_name)
    print('Row count: ', row_count)
    functional_dependencies, columns, candidate_keys, primary_key = get_functional_dependencies(table_name)
    print('Functional dependencies: ', functional_dependencies)
    print('Columns: ', columns)
    print('Candidate keys: ', candidate_keys)
    print('Primary key: ', primary_key)

    print('Lossless join test: ', lossless_join_test(table_name, functional_dependencies, columns))

    synthesized_tables = three_nf_synthesis_dependency_preservation(table_name, functional_dependencies, columns, primary_key)
    print('Synthesized tables by dependency preservation: ')

    for table in synthesized_tables:
        print("Table: ", table['primary_key'])
        print("Columns: ", table['columns'])
    
    synthesized_tables = three_nf_synthesis_lossless_join(table_name, functional_dependencies, columns, primary_key)

    print('Synthesized tables by lossless join: ')

    for table in synthesized_tables:
        print("Table: ", table['primary_key'])
        print("Columns: ", table['columns'])