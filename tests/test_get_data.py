from src.get_data import *
import pandas as pd
import os
# import unittest


def test_fetch_data_returns_dataframe():
    url = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"
    # df = pd.read_csv(url)
    assert isinstance(fetch_data(url),pd.DataFrame)


def test_clean_data_return_dataframe():
    url = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"
    df = pd.read_csv(url).sample(10)
    
    map = os.path.join(os.pardir, 'data', 'fips_code_ref.xlsx')
    df2 = pd.read_excel(map, dtype={'Geo_FIPS': 'Int64', 'Geo_QName': 'str', 'County Pop 2018': 'Int64',
                                    'Geo_STATE': np.int32, 'MSA': 'str', 'Unnamed: 5': 'str',
                                    'Unnamed: 6': 'str', 'MSAs in the US (excluding Puerto Rico)': 'str'})

    assert isinstance(clean_data(df,df2),pd.DataFrame)


def test_clean_data_return_dataframe_with_missing_vals():
    url = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"
    map = os.path.join(os.pardir, 'data', 'fips_code_ref.xlsx')
    
    df = pd.read_csv(url)
    df2 = pd.read_excel(map,dtype={'Geo_FIPS':'Int64','Geo_QName':'str','County Pop 2018':'Int64',
                                           'Geo_STATE':np.int32,'MSA':'str','Unnamed: 5':'str',
                                            'Unnamed: 6':'str','MSAs in the US (excluding Puerto Rico)':'str'})

    clean_df = clean_data(df.sample(10),df2)
    assert clean_df.isna().sum().sum() == 0


# def test_map_county_to_msa_return_dataframe():
#     map = os.path.join(os.pardir,'data','fips_code_ref.xlsx')
#     url = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"
#     df1 = pd.read_csv(url).sample(10)
#     df2 = pd.read_csv(map)
#     assert isinstance(map_county_to_msa(df1,df2),pd.DataFrame)


def test_get_format_return_tuple():
    url = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"
    maps = os.path.join(os.pardir, 'data', 'fips_code_ref.xlsx')
    
    df = pd.read_csv(url)
    df2 = pd.read_excel(maps, dtype={'Geo_FIPS': 'Int64', 'Geo_QName': 'str', 'County Pop 2018': 'Int64',
                                    'Geo_STATE': np.int32, 'MSA': 'str', 'Unnamed: 5': 'str',
                                    'Unnamed: 6': 'str', 'MSAs in the US (excluding Puerto Rico)': 'str'})
    clean_df = clean_data(df.sample(10), df2)
    assert isinstance(get_format(clean_df),tuple)
    

def test_get_format_return_tuple_with_2_elements():
    url = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"
    maps = os.path.join(os.pardir, 'data', 'fips_code_ref.xlsx')
    
    df = pd.read_csv(url)
    df2 = pd.read_excel(maps, dtype={'Geo_FIPS': 'Int64', 'Geo_QName': 'str', 'County Pop 2018': 'Int64',
                                     'Geo_STATE': np.int32, 'MSA': 'str', 'Unnamed: 5': 'str',
                                     'Unnamed: 6': 'str', 'MSAs in the US (excluding Puerto Rico)': 'str'})
    clean_df = clean_data(df.sample(10), df2)
    assert len(get_format(clean_df)) == 2


def test_get_format_return_tuple_of_dataframes():
    url = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"
    maps = os.path.join(os.pardir, 'data', 'fips_code_ref.xlsx')
    
    df = pd.read_csv(url)
    df2 = pd.read_excel(maps, dtype={'Geo_FIPS': 'Int64', 'Geo_QName': 'str', 'County Pop 2018': 'Int64',
                                     'Geo_STATE': np.int32, 'MSA': 'str', 'Unnamed: 5': 'str',
                                     'Unnamed: 6': 'str', 'MSAs in the US (excluding Puerto Rico)': 'str'})
    clean_df = clean_data(df.sample(10), df2)
    df_1, df_2 = get_format(clean_df)
    
    assert isinstance(df_1,pd.DataFrame) and isinstance(df_2,pd.DataFrame)


def test_write_to_csv_returns_bool():
    df1 = pd.DataFrame([1,2,3])
    df2 = pd.DataFrame([4,5,6])
    assert isinstance(write_to_csv(df1, df2, 'df1.csv', 'df2.csv'),bool)


def test_write_to_csv_returns_true():
    df1 = pd.DataFrame([1,2,3])
    df2 = pd.DataFrame([4,5,6])
    assert write_to_csv(df1, df2, 'df1.csv', 'df2.csv') is True
