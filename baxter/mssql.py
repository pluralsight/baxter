#!/usr/bin/env python
import pyodbc
import json
from toolbox import process_data_row
from files import (get_schema_file, loop_delimited_file)
from toolbox import _defaultencode
import logging
log = logging.getLogger(__name__)

def connect(server, database, username, password):
    """Build pyodbc connection to SQL Server from file, assuming driver name is "ODBC Driver 11 for SQL Server"

        Args:
            server: string, name or ip address for SQL Server
            database: string, database name
            username: string, useranme for the database
            password: string, password for the database
    
        Returns:
            pyodbc connection object
    """
    try:
        connect_string = 'DRIVER={FreeTDS};SERVER=' + server.encode('utf-8') + ';PORT=1433;DATABASE=' + database.encode(
            'utf-8') + ';UID=' + username.encode('utf-8') + ';PWD=' + password.encode('utf-8')
        connection = pyodbc.connect(connect_string)
    except (ValueError) as e:
        log.error("Error creating database connection", e)
        raise e

    return connection


def insert_list_to_sql(connection,lst,tableName):
    """Inserts from a list to a SQL table.  List must have the same format and item order as the table columns.
        Args:
            list: list, Values to insert to table
            tableName: string, Fully qualified SQL table name

        Returns:
            None
    """ 
    sorted_column_values_list = []
    for items in lst:
        sorted_column_values_list.append(items)

    for val in sorted_column_values_list:
        valstring = '('
        for colval in val:
            try:
                valstring += "'" + colval + "',"
            except TypeError:
                valstring += str(colval) +','
        valstring = valstring[0:-1] + ')' #remove trailing comma
        query = "INSERT INTO {0} VALUES {1}".format(tableName, valstring)

        c = run_sql(connection,query)
    return

def insert_list_to_sql_batch(connection,lst,tableName,batchsize=1000):
    """Inserts from a list to a SQL table.  List must have the same format and item order as the table columns.
        Args:
            list: list, Values to insert to table
            tableName: string, Fully qualified SQL table name
            batchsize: specifies what size you'd want the batches to run as
            connection: sql server connection

        Returns:
            None
    """ 
    insertvals = ''
    batchcnt = 0
    lstcnt = 0
    lstsize = len(lst)
    rowstr = 'SELECT '
    for row in lst:
        if batchcnt == batchsize or lstcnt == lstsize:
            for val in row:
                if type(val) == int or val == 'null':
                    rowstr += str(val) +','
                else:
                    rowstr += "'" + str(val) + "',"
            insertvals = insertvals + rowstr[:-1] + ' UNION ALL '
            c = run_sql(connection,"INSERT INTO {0} {1}".format(tableName, insertvals[:-11]))
            insertvals = ''
            rowstr = 'SELECT '
            batchcnt = 0
        else:
            for val in row:
                    if type(val) == int or val == 'null':
                        rowstr += str(val) +','
                    else:
                        rowstr += "'" + str(val) + "',"
            insertvals = insertvals + rowstr[:-1] + ' UNION ALL '
            rowstr = 'SELECT '
            batchcnt += 1
            lstcnt += 1

    if batchcnt > 0:
        c = run_sql(connection,"INSERT INTO {0} {1}".format(tableName, insertvals[:-11]))

    return


def run_sql(connection,query): #courseTagDict
    """Runs SQL statement and commits changes to database.
        
        Args:
            connection: pyodbc.connect() object, Connection to use when running Sql 
            query: string, Valid query string

        Returns:
            cursor object, Results of the call to pyodb.connection().cursor().execute(query)
    """ 
    cursor=connection.cursor()
    cursor.execute(query.encode('utf-8'))
    connection.commit()

    return cursor


def truncate_sql_table(connection,table_name):
    """Runs truncate table SQL command and commits changes to database.
        
        Args:
            connection: pyodbc.connect() object, Connection to use for truncate
            tableName: string, Fully qualified SQL table name (make sure this is the table you want to clear!)

        Returns:
            None
    """ 
    sql = "truncate table " + table_name
    cursor=connection.cursor()
    cursor.execute(sql.encode('utf-8'))
    connection.commit()

    return


def create_table(connection, table_name, schema_file):
    """Create table.

        Args:
            connection: pyodbc.connect() object, Connection to use when running Sql
            table_name: string, Table name including db schema (ex: my_schema.my_table)
            schema_file: string, Path to csv schema file with each row as col_name, data_type
        Returns:
            cursor object, Results of the call to pyodb.connection().cursor().execute(query)
    """
    cursor = connection.cursor()
    schema_list = get_schema_file(schema_file)

    table_split = table_name.split('.')
    table = table_split[-1]
    use_db = ""
    if len(table_split) > 1:
        use_db = "USE {0}; ".format(table_split[0])
    ddl = use_db + """IF NOT EXISTS ( SELECT [name] FROM sys.tables WHERE [name] = '{0}' )
        CREATE TABLE {0} (""".format(table_name)
    for col, dt in schema_list:
        ddl = ddl + col + ' ' + dt + ' NULL, '
    ddl = ddl[:-2] + ');'

    try:
        log.debug(ddl)
        cursor.execute(ddl.encode('utf-8'))
    except UnicodeDecodeError:
        cursor.execute(ddl)
    return cursor

def create_index(connection, table_name, index):
    """Create index.

            Args:
                connection: pyodbc.connect() object, Connection to use when running Sql
                table_name: string, Table name including db schema (ex: my_schema.my_table)
                index: string, Column name of index (can put multiple columns comma delimited if desired)
            Returns:
                cursor object, Results of the call to pyodb.connection().cursor().execute(query)
        """
    cursor = connection.cursor()
    table_split = table_name.split('.')
    table = table_split[-1]
    if len(table_split) > 1:
        use_db = "USE {0}; ".format(table_split[0])
        run_sql(connection, use_db)
    if index is not None:
        idx_name = table + '_idx'
        sql = "SELECT name FROM sys.indexes where name = '{0}' and object_id = OBJECT_ID('{1}')".format(idx_name, table)
        log.debug("SQL to run: " + sql)
        try:
            exists = sql_get_query_data(connection, sql)
            val = exists.fetchone()[0]
            if val != idx_name:
                ddl2 = 'CREATE INDEX {0} ON {1}({2});'.format(idx_name, table_name, index)
                try:
                    cursor.execute(ddl2.encode('utf-8'))
                    connection.commit()
                except UnicodeDecodeError:
                    cursor.execute(ddl2)
                    connection.commit()
        except TypeError:
            log.info("Index does not exist, will attempt to create it")
            ddl2 = 'CREATE INDEX {0} ON {1}({2});'.format(idx_name, table_name, index)
            try:
                cursor.execute(ddl2.encode('utf-8'))
                connection.commit()
            except UnicodeDecodeError:
                cursor.execute(ddl2)
                connection.commit()

    return cursor



def sql_get_schema(connection,query,include_extract_date = True):
    """Reads schema from database by running the provided query.  It's recommended to
    pass a query that is limited to 1 record to minimize the amount of rows accessed on 
    the server.

        Args:
            connection: pyodbc.connect() object, Connection to use when running Sql 
            query: string, Valid query string
            include_extract_date: boolean, defaults to True to add current timestamp field 
                    'ExtractDate' to results

        Returns:
            list, each list item contains field name and data type
    """
    import json

    cursor = connection.cursor()
    cursor.execute(query)
    
    schema_list = []
    #colList = []
    #typeList = []
    for i in cursor.description:
        schema_list.append(i[0:2])
        #colList.append(i[0])
        #typeList.append(str(i[1]))
    if include_extract_date:
        schema_list.append(['ExtractDate','datetime'])
    return schema_list


def sql_get_table_data(connection, table, schema='dbo', include_extract_date = True):
    """Runs SQL statement to get all records from the table (select *)
        
        Args:
            connection: pyodbc.connect() object, Connection to use when selecting data 
            table: string, Valid table

        Returns:
            cursor object, Results of the call to pyodb.connection().cursor().execute(query)
    """ 
    extract_date = ""
    if include_extract_date:
        extract_date = ", getdate() as ExtractDate"
    query = 'select * ' + extract_date + ' from ' + schema + '.[' + table + '] with (nolock)'
    log.info(query)
    cursor=connection.cursor()
    cursor.execute(query.encode('utf-8'))

    return cursor


def sql_get_query_data(connection, query):
    """Runs SQL statement to get results of query specified, returned and pyodbc cursor.
        
        Args:
            connection: pyodbc.connect() object, Connection to use when selecting data 
            query: string, Valid select statement

        Returns:
            cursor object, Results of the call to pyodb.connection().cursor().execute(query)
    """ 
    cursor=connection.cursor()
    cursor.execute(query.encode('utf-8'))

    return cursor


def cursor_to_json(cursor, dest_file, dest_schema_file=None, source_schema_file=None):
    """Takes a cursor and creates JSON file with the data 
    and a schema file for loading to other data systems.

    Args:
        cursor: cursor object with data to extract to file
        dest_file: string, path and file name to save data

    Returns:
        None
    """
    if source_schema_file is None:
        schema = []
        for i in cursor.description:
            schema.append([i[0],str(i[1])])
    else:
        schema = get_schema_file(source_schema_file)

    if dest_schema_file is not None:
        with open(dest_schema_file,'wb') as schemafile:
            for row in schema:
                col = row[0]
                if 'date' in row[1]:
                    datatype = 'timestamp'
                elif 'list' in row[1]:
                    datatype = 'list'
                elif 'bigint' in row[1]:
                    datatype = 'bigint'
                elif 'int' in row[1] or 'long' in row[1]:
                    datatype = 'integer'
                elif 'float' in row[1]:
                    datatype = 'float'
                elif 'bool' in row[1]:
                    datatype = 'boolean'
                elif 'str' in row[1]:
                    datatype = 'string'
                else:
                    datatype = 'string'
                schemafile.write("%s\n" % (col + ',' + datatype))

    with open(dest_file,'wb') as outfile:
        for row in cursor:
            result_dct = process_data_row(row,schema)
            outfile.write("%s\n" % json.dumps(result_dct, default=_defaultencode))

def load_csv_to_table(table ,schema_file ,csv_file, server, database, config,cred_file='config/dblogin.config',skipfirstrow=1):
    """Takes csv file, schema file, with sql server connection params and inserts data to a specified table

    Args:
        table: table name where csv data will be written
        schema_file: schema file that has all column names and data type names
        csv_file: data being loaded
        server: sql server host name
        config: which configuration name to pull username and password credentials
        cred_file: location of db login config file
        skipfirstrow(optional): if 1 then skip the first row of data (exclude headers)

    Returns:
        None
    """    
    from files import loop_csv_file, loop_delimited_file
    from files import get_schema_file

    with open(cred_file,'rb') as cred:
        db_info = json.loads(cred.read())

    username = db_info[config]['username']
    password = db_info[config]['password']

    data_list = loop_csv_file(csv_file)

    connection = mssql_connect(server, database, username, password)

    schema_list = get_schema_file(schema_file)
    #skips the first value of data_list which is the header
    data_list = iter(data_list)
    if skipfirstrow == 1:
        next(data_list)

    insert_datarows_to_table(data_list, schema_list, connection, table)


def load_delimited_file_to_table(connection, table , source_file, schema_file, skipfirstrow=1, delimiter=','):
    """Takes delimited file name, schema file, and db connection and inserts data to a specified table

    Args:
        table: table name where csv data will be written
        schema_file: schema file that has all column names and data type names
        csv_file: data being loaded
        server: sql server host name
        config: which configuration name to pull username and password credentials
        cred_file: location of db login config file
        skipfirstrow(optional): if 1 then skip the first row of data (exclude headers)

    Returns:
        None
    """
    data_list = loop_delimited_file(source_file,delimiter=delimiter)
    schema_list = get_schema_file(schema_file)
    #skips the first value of data_list which is the header
    data_list = iter(data_list)
    if skipfirstrow == 1:
        next(data_list)
    insert_datarows_to_table(data_list,schema_list,connection,table)

def insert_datarows_to_table(data_list, schema_list, connection, table):
    """gets a data list and converts it to the correct data type for inserts then inserts data to a table

    Args:
        data_list: a list of lists which contain data row Values
        schema_list: a list of lists which contains all the column names with their respective data type

    Returns:
        None
    """
    insert_list = []
    for i in data_list:
        load_list = []
        for j, val in enumerate(i):
            if 'int' in schema_list[j][1]:
                if val == 'null' or val == '':
                    load_list.append('null')
                else:
                    load_list.append(int(val))
            elif 'date' in schema_list[j][1]:
                load_list.append(str(val)[:19])
            else:
                load_list.append(str(val).replace("'","''"))
        insert_list.append(load_list)

    insert_list_to_sql_batch(connection, insert_list, table,100)








