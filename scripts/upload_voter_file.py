import psycopg2
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

# Define a database name (we're using a dataset on births, so we'll call it birth_db)
# Set your postgres username
dbname = 'map_the_vote'
username = 'docker'
password = 'docker'

engine = create_engine('postgres://%s:%s@localhost/%s'%(username,password,dbname))
print(engine.url)

## create a database (if it doesn't exist)
if not database_exists(engine.url):
    print('Creating database')
    create_database(engine.url)
print(database_exists(engine.url))

# Connect to make queries using psycopg2
con = psycopg2.connect(host='localhost', database = dbname, user = username, password=password)


def perform_pandas_cleaning(dataframe):
    print('Initial Dataset size:', len(dataframe.index))

    print('Standardize street names')
    dataframe['vf_street_address'].replace(regex='COUNTY ROAD', value='CR', inplace=True)
    dataframe['vf_street_address'].replace(regex='STATE ROAD', value='SR', inplace=True)
    dataframe['vf_street_address'].replace(regex='RANCH ROAD', value='RR', inplace=True)
    dataframe['vf_street_address'].replace(regex='MC ', value='MC', inplace=True)
    dataframe['vf_street_address'].replace(regex='  ', value=' ', inplace=True)

    print('Cleaned Dataset size:', len(dataframe.index))

    return dataframe


# This is the original cleaning methodology, preserving here for now as reference
def perform_sql_cleaning():
    sql_query = """
    UPDATE %s SET vf_street_address = replace(vf_street_address, 'COUNTY ROAD', 'CR');
    """ % (table_name)
    engine.execute(sql_query)

    sql_query = """
    UPDATE %s SET vf_street_address = replace(vf_street_address, 'RANCH ROAD', 'RR');
    """ % (table_name)
    engine.execute(sql_query)

    sql_query = """
    UPDATE %s SET vf_street_address = replace(vf_street_address, 'STATE ROAD', 'SR');
    """ % (table_name)
    engine.execute(sql_query)

    sql_query = """
    UPDATE %s SET vf_street_address = replace(vf_street_address, 'HIGHWAY', 'HWY')
    """ % (table_name)
    engine.execute(sql_query)

    sql_query = """
    UPDATE %s SET vf_street_address = replace(vf_street_address, '  ', ' ');
    """ % (table_name)
    engine.execute(sql_query)

    sql_query = """
    UPDATE %s SET vf_street_address = replace(vf_street_address, 'MC ', 'MC')
    """ % (table_name)
    engine.execute(sql_query)


voter_file_all = pd.DataFrame()
voter_files = ['voter_file_tx_10.txt', 'voter_file_tx_21.txt', 'voter_file_tx_25.txt']

for vf in voter_files:
    voter_file = pd.read_table('data/'+vf,
                               sep='\t',
                               header=0,
                               dtype={"Voter File VANID": str, "Sex": str, "Age": str, #"DateReg": str,
                                      "StreetPrefix": str, "StreetNo": str, "StreetName": str, "StreetType": str,
                                      "AptType": str, "AptNo": str, "City": str, "State": str,
                                      "Zip5": str, "PrecinctName": str, "CountyName": str}, keep_default_na=False)

    print('Processing voter file {0}'.format(vf))
    voter_file.columns = voter_file.columns.str.strip()

    voter_file.PrecinctName = voter_file.PrecinctName.astype(str)

    voter_file = voter_file[["Voter File VANID", "Sex", "Age", "StreetPrefix", "StreetNo", "StreetName", "StreetType",
                             "AptType", "AptNo", "City", "State", "Zip5", "PrecinctName", "CountyName"]]

    voter_file.columns = map(str.lower, voter_file.columns)
    voter_file.columns = voter_file.columns.str.replace(" ", "_")
    voter_file.columns = ['vf_' + str(col) for col in voter_file.columns]

    # some preprocessing steps that will help later
    voter_file["vf_street_address"] = voter_file["vf_streetno"] + ' ' + voter_file["vf_streetprefix"] + ' ' + voter_file["vf_streetname"] + ' ' +  voter_file["vf_streettype"] + ' ' +  voter_file["vf_zip5"]
    voter_file["vf_street_address_no_prefix"] = voter_file["vf_streetno"] + ' ' + voter_file["vf_streetname"] + ' ' +  voter_file["vf_streettype"] + ' ' +  voter_file["vf_zip5"]
    voter_file["vf_street_address_no_type"] = voter_file["vf_streetno"] + ' ' + voter_file["vf_streetname"] + ' ' +  voter_file["vf_zip5"]
    voter_file["vf_residential"] = "y"
    voter_file["vf_street_address"] = voter_file["vf_street_address"].str.upper()
    voter_file["vf_street_address_no_prefix"] = voter_file["vf_street_address_no_prefix"].str.upper()

    # add multi-unit field
    voter_file.loc[(voter_file['vf_apttype'].str.len() > 0),'vf_multi_unit'] = "y"
    voter_file.loc[(voter_file['vf_apttype'].str.len() == 0),'vf_multi_unit'] = "n"

    # create cntyvtd field for join with precinct shapefile, TX 10, TX 21, TX 25
    # TODO add other precinct counties as needed
    voter_file['vf_PrecinctNamePad'] = voter_file['vf_precinctname'].apply(lambda x: str(x).zfill(4))
    voter_file.loc[voter_file['vf_countyname'] == "Austin",'vf_CountyCode']  = '15'
    voter_file.loc[voter_file['vf_countyname'] == "Bandera",'vf_CountyCode']  = '19'
    voter_file.loc[voter_file['vf_countyname'] == "Bastrop",'vf_CountyCode']  = '21'
    voter_file.loc[voter_file['vf_countyname'] == "Bell",'vf_CountyCode']  = '27'
    voter_file.loc[voter_file['vf_countyname'] == "Bexar",'vf_CountyCode']  = '29'
    voter_file.loc[voter_file['vf_countyname'] == "Blanco",'vf_CountyCode']  = '31'
    voter_file.loc[voter_file['vf_countyname'] == "Bosque",'vf_CountyCode']  = '35'
    voter_file.loc[voter_file['vf_countyname'] == "Burnet",'vf_CountyCode']  = '53'
    voter_file.loc[voter_file['vf_countyname'] == "Colorado",'vf_CountyCode']  = '89'
    voter_file.loc[voter_file['vf_countyname'] == "Comal",'vf_CountyCode']  = '91'
    voter_file.loc[voter_file['vf_countyname'] == "Coryell",'vf_CountyCode']  = '99'
    voter_file.loc[voter_file['vf_countyname'] == "Erath",'vf_CountyCode']  = '143'
    voter_file.loc[voter_file['vf_countyname'] == "Fayette",'vf_CountyCode']  = '149'
    voter_file.loc[voter_file['vf_countyname'] == "Gillespie",'vf_CountyCode']  = '171'
    voter_file.loc[voter_file['vf_countyname'] == "Harris",'vf_CountyCode']  = '201'
    voter_file.loc[voter_file['vf_countyname'] == "Hamilton",'vf_CountyCode']  = '193'
    voter_file.loc[voter_file['vf_countyname'] == "Hays",'vf_CountyCode']  = '209'
    voter_file.loc[voter_file['vf_countyname'] == "Hill",'vf_CountyCode']  = '217'
    voter_file.loc[voter_file['vf_countyname'] == "Kendall",'vf_CountyCode']  = '259'
    voter_file.loc[voter_file['vf_countyname'] == "Johnson",'vf_CountyCode']  = '251'
    voter_file.loc[voter_file['vf_countyname'] == "Kerr",'vf_CountyCode']  = '265'
    voter_file.loc[voter_file['vf_countyname'] == "Lampasas",'vf_CountyCode']  = '281'
    voter_file.loc[voter_file['vf_countyname'] == "Lee",'vf_CountyCode']  = '287'
    voter_file.loc[voter_file['vf_countyname'] == "Real",'vf_CountyCode']  = '385'
    voter_file.loc[voter_file['vf_countyname'] == "Somervell",'vf_CountyCode']  = '425'
    voter_file.loc[voter_file['vf_countyname'] == "Tarrant",'vf_CountyCode']  = '439'
    voter_file.loc[voter_file['vf_countyname'] == "Travis",'vf_CountyCode']  = '453'
    voter_file.loc[voter_file['vf_countyname'] == "Waller",'vf_CountyCode']  = '473'
    voter_file.loc[voter_file['vf_countyname'] == "Washington",'vf_CountyCode']  = '477'
    voter_file['vf_cntyvtd'] = voter_file['vf_CountyCode'] + voter_file['vf_PrecinctNamePad']

    print('Voter records count:', len(voter_file.index))
    voter_file_all = voter_file_all.append(voter_file)

print('Total voter records count:', len(voter_file_all.index))
voter_file_all = perform_pandas_cleaning(voter_file_all)

print('Create voter file table')
table_name = 'voter_file_all'
voter_file_all.to_sql(table_name, engine, if_exists='replace')


