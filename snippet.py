import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd

def get_psql_connector(host, port, database, user, password, schema='public'):
    port = str(port)
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(user,password,host,port,database))
    try:
        c = engine.connect()
    except sqlalchemy.exc.OperationalError as e:
        raise Exception('Connection failed with the provided credentials')
    meta = sqlalchemy.MetaData(engine, schema=schema)
    meta.reflect(engine, schema=schema)
    pdsql = pd.io.sql.SQLDatabase(engine, meta=meta)
    return pdsql

if __name__ == "__main__":

    connector = get_psql_connector("localhost", 5432, "database", "username", "password")
    df = pd.DataFrame([[1,'a'],[2,'b']],columns = ['num','letter'])
    connector.to_sql(df,"firstTable")
    #todo: close the connection?

