import pandas as pd
import numpy as np
import os
import subprocess
from datetime import datetime as dt
import time

def fetch_data(url):
    """
    Gets data from New York Times Git Repository
    :param url (str): URL of csv file containing COVID data.
    :return: DataFrame
    """
    return pd.read_csv(url,dtype={'date':'str','county':'str','state':'str',
                                           'fips':'Int64','cases':np.int32,'death':np.int32})

def clean_data(df1,df2):
    """
    Cleans data from CSV file containing COVID19 cases and deaths
    :param df1 (DataFrame): DataFrame containing COVID cases and deaths by county
    :param df2 (DataFrame): DataFrame containing county to msa relationships using FIPS code
    :return: DataFrame
    """

    def get_msa(fips_code):
        try:
            return df2['MSA'][df2['Geo_FIPS'] == fips_code].values[0]
        except:
            return "NaN"
        
    UnknownCounty_df = df1[(df1['county'] == "Unknown")].copy()
    # UnknownCounty_df.head()

    # Fixing New York City County problem
    NewYorkCityCounty_df = df1[(df1['county'] == "New York City") & (df1['state'] == "New York")].copy()
    NewYorkCityCounty_df['fips'] = 36061
    NewYorkCityCounty_df['MSA'] = "New York-Newark-Jersey City, NY-NJ-PA"

    # Removing New York City County from raw dataframe
    to_drop = df1[((df1['county'] == "New York City") & (df1['state'] == "New York"))].index
    df1.drop(index=to_drop, inplace=True)

    KansasCity_df = df1[(df1['county'] == "Kansas City") & (df1['state'] == "Missouri")].copy()
    KansasCity_df['fips'] = 29095
    KansasCity_df['MSA'] = "Kansas City, MO-KS"

    # Removing Kansas City County from raw dataframe
    to_drop = df1[((df1['county'] == "Kansas City") & (df1['state'] == "Missouri"))].index
    df1.drop(index=to_drop, inplace=True)

    # Adding msa to other counties

    df1['MSA'] = df1['fips'].apply(lambda x: get_msa(x))

    df1.dropna(inplace=True)
    
    return pd.concat([df1,NewYorkCityCounty_df,KansasCity_df])


def get_format(df, isUnallocated=False):
    """
    Abstracts cases and deaths by msas into two different dataframes
    :param df(DataFrame): DataFrame containing cases and deaths by msas
    :param isUnallocated(bool): Bool to determine if we are interested in counties with no msa allocation.
    :return: DataFrame, DataFrame
    """
    dates = sorted(list(set(df['date'])))
    case_df = {}
    death_df = {}

    if isUnallocated:
        states = list(set(df['state']))
    
        for state in states:
            df1 = df[df['state'] == state]
            date_cases = {}
            date_deaths = {}
        
            for date in dates:
                cases = df1[['cases']][df1['date'] == date].sum()[0]
                deaths = df1[['deaths']][df1['date'] == date].sum()[0]
            
                date_cases[date] = cases
                date_deaths[date] = deaths
        
            case_df[state] = date_cases
            death_df[state] = date_deaths
    
        msas_cases, msas_deaths = pd.DataFrame(case_df), pd.DataFrame(death_df)
        return msas_cases, msas_deaths
    else:
        msas = list(set(df['MSA']))
        for msa in msas:
            df1 = df[df['MSA'] == msa]
            date_cases = {}
            date_deaths = {}
        
            for date in dates:
                cases = df1[['cases']][df1['date'] == date].sum()[0]
                deaths = df1[['deaths']][df1['date'] == date].sum()[0]
            
                date_cases[date] = cases
                date_deaths[date] = deaths
        
            case_df[msa] = date_cases
            death_df[msa] = date_deaths
    
        msas_cases, msas_deaths = pd.DataFrame(case_df), pd.DataFrame(death_df)
        #         return msas_cases, msas_deaths
        return msas_cases.reindex(sorted(msas_cases.columns), axis=1).T, msas_deaths.reindex(
            sorted(msas_deaths.columns), axis=1).T


def write_to_csv(df1,df2,path1,path2):
    """
    Writes data of cases and deaths to csv
    :param df1(DataFrame): DataFrame containing cases by msas
    :param df2(DataFrame): DataFrame containing deaths by msas
    :param path1(str): Filepath to location where cases csv will be saved
    :param path2(str): Filepath to location where deaths csv will be saved
    :return: Bool
    """
    # df1['key'] = range(len(df1.index))
    df1['msas'] = df1.index
    df1.set_index(keys='msas', inplace=True)

    # new_col = ['msas']
    # new_col.extend(list(df1.columns.values)[:-1])

    # df1 = df1[new_col]

    # df2['key'] = range(len(df2.index))
    df2['msas'] = df2.index
    df2.set_index(keys='msas', inplace=True)
    #
    # new_col = ['msas']
    # new_col.extend(list(df2.columns.values)[:-1])
    #
    # df2 = df2[new_col]
    
    try:
        df1.to_csv(path1)
        df2.to_csv(path2)
        
    except:
        raise

    return True


def commit_to_git():
    subprocess.call('git add .', shell=True)
    # time.sleep(3)
    subprocess.call(f'git commit -m "Data updated as at {dt.now().strftime("%d-%m-%Y")}"', shell=True)
    # time.sleep(3)
    subprocess.call('git push', shell=True)
    # time.sleep(3)
    
    
def main():
    # url = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"
    # maps = os.path.join(os.pardir, 'data', 'fips_code_ref.xlsx')
    # path_cases = os.path.join(os.pardir,'data','all_msas_cases.csv')
    # path_deaths = os.path.join(os.pardir, 'data', 'all_msas_deaths.csv')
    #
    # raw_df = fetch_data(url)
    # fips_df = pd.read_excel(maps, dtype={'Geo_FIPS': 'Int64', 'Geo_QName': 'str', 'County Pop 2018': 'Int64',
    #                                  'Geo_STATE': np.int32, 'MSA': 'str', 'Unnamed: 5': 'str',
    #                                  'Unnamed: 6': 'str', 'MSAs in the US (excluding Puerto Rico)': 'str'})
    #
    # all_msas_clean_df = clean_data(raw_df,fips_df)
    #
    # msa_cases, msa_deaths = get_format(all_msas_clean_df)
    #
    # write_to_csv(msa_cases,msa_deaths,path_cases,path_deaths)

    commit_to_git()

if __name__ == '__main__':
    main()
