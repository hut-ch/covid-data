import os
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text

def create_db_conn() -> sqlalchemy.engine:
    # get db credentials from environment file for a bit of security
    load_dotenv('db.env')

    # get all the environmentt variable
    host = os.getenv('host_name')
    port = os.getenv('POSTGRES_PORT')
    user = os.getenv('POSTGRES_USER')
    pw = os.getenv('POSTGRES_PW')
    db = 'covid-data'

    db_url ='postgresql+psycopg2://'+user+':'+pw+'@'+host+':'+port+'/'+db

    # create connection
    try:
        engine = create_engine(db_url)
    except Exception as e:
        print('Unable to access postgresql database', repr(e))

    return engine
    
def create_tables():

    # Open and read the file as a single buffer
    with open('create_tables.sql', 'r') as f:
        sql_tables = f.read()

    # all SQL commands (split on ';')
    tables = sql_tables.split(';')

    #create connection to db
    engine = create_db_conn()
    con = engine.connect()

    # Execute every command from the input file
    for table in tables:
        # This will skip and report errors
        try:
            query = text(table)
            con.execute(query)
        except OperationalError, msg:
            print("Command skipped: ", msg)