import pandas as pd
import pyodbc
import os
import urllib
from sqlalchemy import create_engine
from fast_to_SQL import fast_to_SQL as fts

def get_msa_data():
    """
    Returns a dataframe of cases and deaths by MSAs
    :return: DataFrame, DataFrame
    """
    cases_path = os.path.join(os.pardir,'data', 'all_msas_cases.csv')
    deaths_path = os.path.join(os.pardir, 'data', 'all_msas_deaths.csv')
    return pd.read_csv(cases_path, index_col='msas'), pd.read_csv(deaths_path, index_col='msas')


def is_db_credentials_defined():
    """
    Checks if the Database parameters are defined
    :return: Bool
    """
    return True if os.getenv('MARRON_DB') and os.getenv('MARRON_PASSWORD') and os.getenv('MARRON_SERVER') \
                   and os.getenv('MARRON_USER') else False

def is_db_accessible():
    """
    Check if the Database is accessible
    :return:
    """
    server = os.getenv('MARRON_SERVER')
    database = os.getenv('MARRON_DB')
    username = os.getenv('MARRON_USER')
    password = os.getenv('MARRON_PASSWORD')
    driver = '{ODBC Driver 17 for SQL Server}'
    try:
        cnxn = pyodbc.connect(
            'DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database + ';Uid=' + username + ';Pwd=' + password + ';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
        cursor = cnxn.cursor()
        return True
        
    except:
        raise
    finally:
        cnxn.close()
    

def get_db_engine():
    """
    Returns SQLAlchemy engine for querying db
    :return:
    """

    server = os.getenv('MARRON_SERVER')
    database = os.getenv('MARRON_DB')
    username = os.getenv('MARRON_USER')
    password = os.getenv('MARRON_PASSWORD')
    driver = '{ODBC Driver 17 for SQL Server}'

    params = urllib.parse.quote_plus(
        'DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database + ';Uid=' + username + ';Pwd=' + password + ';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    
    try:
        return create_engine(conn_str, echo=False, fast_executemany=True)
    except:
        raise
    
def upload_to_db(df,table,engine):
    """
    
    :param df(DataFrame): DataFrame to upload
    :param table(str): A string representing the name of the table to upload data to
    :param engine(sqlalchemy.Engine): An SQLAchemy engine
    :return: None
    """
    
    try:
        fts.to_sql_fast(df=df, name=table, engine=engine, if_exists='replace')
    except fts.FailError:
        raise
    finally:
        engine.dispose()
        
        
def main():
    cases, deaths = get_msa_data()
    
    
    if is_db_credentials_defined() and is_db_accessible():
        engine = get_db_engine()
        upload_to_db(cases, 'cases', engine)
        upload_to_db(cases, 'deaths', engine)
        print('Done')
    
    else:
        print("DB credential is undefined or DB is inaccessible")
    
    return None


if __name__ == '__main__':
    main()
    