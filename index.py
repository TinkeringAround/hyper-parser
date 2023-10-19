import shutil, csv, sys, os

from pathlib import Path
from tableauhyperapi import HyperProcess, Telemetry, Connection, TableName, HyperException

def getTableName(db: Connection, schema = "Extract"):
    """
    Retrieves the default table name of a Hyper File Extract
    """
    tableNames = db.catalog.get_table_names(schema)

    if tableNames.count == 0:
        raise Exception("Tables for schema '{schema}' not found")
    
    return tableNames.pop()

def getColumns(db: Connection, name: TableName):
    """
    Collecting Columns from a Table
    """   
    columns = []

    try:
        tableDefinition = db.catalog.get_table_definition(name)

        for column in tableDefinition.columns:
            print(f"{column.name}: {column.type}")
            columns.append(column.name.unescaped)

    except HyperException as ex:
        print(ex)
        return

    return columns

def writeToCSV(row):
    with open('extract.csv', 'a', newline='') as file:
        writer = csv.writer(file, dialect="excel")
        writer.writerow(row)
        file.close()
    return

def copyDbToCSV(db: Connection, tableName: TableName):
    """
    Reads the Hpyer DB row by rows and copies the rows to a csv
    """
    rowsCount = db.execute_scalar_query(query=f"SELECT COUNT(*) FROM {tableName}")
    limit = 100000
    steps = int(((rowsCount - (rowsCount % limit)) / limit) + 1)
    print(f"Counting {rowsCount} rows, beeing copied in chunks of {limit} in {steps} steps")
    
    for step in range(steps):
        print(f"Copying chunk {step + 1} of {steps}")
        query = f"SELECT * FROM {tableName} ORDER BY kpi_date DESC LIMIT {limit} OFFSET {step * limit};"
        rows = db.execute_list_query(query)
        
        for row in rows:
            output = []
            for column in row:
                output.append(column)
            writeToCSV(output)

if __name__ == '__main__':
    try:
         # Read Hyper Store
        src = Path(sys.argv[1])

        # Make a copy of the superstore denormalized sample Hyper file
        database = Path(shutil.copy(src, dst="db.hyper")).resolve()
        
        # Create Hyper Process and init Connection
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyperProcess:

            # Connect to existing Hyper file
            with Connection(endpoint=hyperProcess.endpoint, database=database) as db:
                tableName = getTableName(db)
                columns = getColumns(db, tableName)
                writeToCSV(columns)
                copyDbToCSV(db, tableName)
                db.close()

            hyperProcess.close()
        
        # Delete tmp Database
        os.remove(database)

    except HyperException as ex:
        print(ex)
        exit(1)