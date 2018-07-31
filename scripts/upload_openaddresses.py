import os
import glob
import psycopg2
import pandas as pd
import geopandas as gpd

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry import Point

# Define a database name (we're using a dataset on births, so we'll call it birth_db)
# Set your postgres username
dbname = 'map_the_vote'
username = 'docker'
password = 'docker'

engine = create_engine('postgres://%s:%s@localhost/%s'%(username,password,dbname))
print(engine.url)

## create a database (if it doesn't exist)
if not database_exists(engine.url):
    create_database(engine.url)
print(database_exists(engine.url))

# Connect to make queries using psycopg2
con = psycopg2.connect(host='localhost', database = dbname, user = username, password=password)

path = os.getcwd() + '/data/openaddr-collected-us_south/us/tx/'
extension = 'csv'
os.chdir(path)
files = [i for i in glob.glob('*.{}'.format(extension))]
files = [path + s for s in files]
files = sorted(files)


def perform_pandas_cleaning(dataframe):
    print('Initial Dataset size:', len(dataframe.index))

    print('Remove blank and zero street numbers and blank street names')
    dataframe = dataframe.loc[(dataframe['oa_number'] != '0') &
                              (dataframe['oa_number'] != '') &
                              (dataframe['oa_street'] != '')]

    print('Create Street Addresses')
    dataframe['oa_street_address'] = dataframe["oa_number"].map(str) + ' ' + \
                                     dataframe['oa_street'] + ' ' + \
                                     dataframe['oa_postcode']

    print('Standardize street names')
    dataframe['oa_street_address'].replace(regex='COUNTY ROAD', value='CR', inplace=True)
    dataframe['oa_street_address'].replace(regex='STATE ROAD', value='SR', inplace=True)
    dataframe['oa_street_address'].replace(regex='RANCH ROAD', value='RR', inplace=True)
    dataframe['oa_street_address'].replace(regex='MC ', value='MC', inplace=True)
    dataframe['oa_street_address'].replace(regex='  ', value=' ', inplace=True)

    print('Cleaned Dataset size:', len(dataframe.index))

    return dataframe


def post_load_cleaning():
    print('update openaddress zips with matches from zip shapefile')
    sql_query = """
    UPDATE addresses_table_tx dst
    SET oa_postcode = src.zcta5ce10
    FROM zip5_us src
    WHERE ST_Intersects(src.geom, dst.geom);
    """
    engine.execute(sql_query)

    print('remove duplicates, just take first one')
    sql_query = """
    CREATE INDEX street_address_idx ON addresses_table_tx (oa_street_address);
    """
    engine.execute(sql_query)

    sql_query = """
    DROP TABLE IF EXISTS addresses_table_tx_no_dupes;
    """
    engine.execute(sql_query)

    sql_query = """
    CREATE TABLE addresses_table_tx_no_dupes AS
    SELECT DISTINCT ON (oa_street_address) * FROM addresses_table_tx;
    """
    engine.execute(sql_query)

    print('Delete the shorter street name (i.e. no prefix)')
    sql_query = """
    DELETE FROM addresses_table_tx_no_dupes a USING addresses_table_tx_no_dupes b
        WHERE LENGTH(a.oa_street) < LENGTH(b.oa_street)
        AND a.oa_postcode = b.oa_postcode
        AND a.oa_number = b.oa_number
        AND (a.oa_street LIKE '%%' || b.oa_street OR b.oa_street LIKE '%%' || a.oa_street);
    """
    engine.execute(sql_query)


# This is the original cleaning methodology, preserving here for now as reference
def perform_sql_cleaning():
    print('Update street_address column in addresses_table_tx')
    sql_query = """
    UPDATE addresses_table_tx
    SET oa_street_address = oa_street_address || ' ' || oa_postcode;
    """
    engine.execute(sql_query)

    print('Delete rows where street number is empty')
    sql_query = """
    DELETE FROM addresses_table_tx
    WHERE oa_number = '';
    """
    engine.execute(sql_query)

    print('Delete rows where street number is zero')
    sql_query = """
    DELETE FROM addresses_table_tx
    WHERE oa_number = '0';
    """
    engine.execute(sql_query)

    print('Delete rows where street is empty')
    sql_query = """
    DELETE FROM addresses_table_tx
    WHERE oa_street = '';
    """
    engine.execute(sql_query)

    print('COUNTY ROAD to CR in openaddress_street_address')
    sql_query = """
    UPDATE addresses_table_tx SET oa_street_address = replace(oa_street_address, 'COUNTY ROAD', 'CR');
    """
    engine.execute(sql_query)

    print('STATE ROAD to SR in openaddress_street_address')
    sql_query = """
    UPDATE addresses_table_tx SET oa_street_address = replace(oa_street_address, 'STATE ROAD', 'SR');
    """
    engine.execute(sql_query)

    print('RANCH ROAD to RR in openaddress_street_address')
    sql_query = """
    UPDATE addresses_table_tx SET oa_street_address = replace(oa_street_address, 'RANCH ROAD', 'RR');
    """
    engine.execute(sql_query)

    print('HIGHWAY to HWY in openaddress_street_address')
    sql_query = """
    UPDATE addresses_table_tx SET oa_street_address = replace(oa_street_address, 'HIGHWAY', 'HWY');
    """
    engine.execute(sql_query)

    print('replace mc _ with mc_ in openaddress_street_address')
    sql_query = """
    UPDATE addresses_table_tx SET oa_street_address = replace(oa_street_address, 'MC ', 'MC')
    """
    engine.execute(sql_query)

    print('get rid of double spaces in openaddress_street_address, run twice')
    sql_query = """
    UPDATE addresses_table_tx SET oa_street_address = replace(oa_street_address, '  ', ' ');
    """
    engine.execute(sql_query)

    print('run it again, just to be sure.')
    sql_query = """
    UPDATE addresses_table_tx SET oa_street_address = replace(oa_street_address, '  ', ' ');
    """
    engine.execute(sql_query)


TABLE_NAME = "addresses_table_tx"
CHUNK_SIZE = 1000
if_exists_strategy='replace'
for file in files: # skip the first one since I read it in already
    print(file)
    address_data = pd.read_csv(file, dtype={"LON": float, "LAT": float, "NUMBER": str, "STREET": str, "UNIT": str,
                                            "CITY": str, "DISTRICT": str, "REGION": str, "POSTCODE": str, "ID": str,
                                            "HASH": str}, keep_default_na=False)

    print('combine text LON and LAT fields into a geometry')
    geometry = [Point(xy) for xy in zip(address_data.LON, address_data.LAT)]

    print('add oa prefix to all columns')
    address_data.columns = ['oa_' + str(col) for col in address_data.columns]
    address_data_gd = gpd.GeoDataFrame(address_data, crs=4326, geometry=geometry)

    print('convert column names to lower case will help later with database queries')
    address_data_gd.columns = map(str.lower, address_data_gd.columns)

    print('Clean data')
    address_data_gd = perform_pandas_cleaning(address_data_gd)

    # this is a bit slow, takes about 30-60s
    # I don't know why this is necessary, but it is
    print('Create "geom" column')
    address_data_gd['geom'] = address_data_gd['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))
    print('Drop "geometry" column')
    address_data_gd.drop('geometry', 1, inplace=True)

    print('write first 1000 rows to database')
    address_data_gd.head(CHUNK_SIZE).to_sql(TABLE_NAME, engine, if_exists=if_exists_strategy, index=False,
                                            dtype={'geom': Geometry('POINT', srid= 4326)})

    if if_exists_strategy == 'replace':
        if_exists_strategy = 'append'

    for i in range(CHUNK_SIZE, address_data_gd.shape[0]+CHUNK_SIZE, CHUNK_SIZE):
        if not i % 10000:
            print('Rows - ', i)
        address_data_gd[i:(i+CHUNK_SIZE)].to_sql(TABLE_NAME, engine, if_exists=if_exists_strategy, index=False,
                                                 dtype={'geom': Geometry('POINT', srid= 4326)})


post_load_cleaning()
