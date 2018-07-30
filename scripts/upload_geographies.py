# import psycopg2
# import pandas as pd
import geopandas as gpd

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from geoalchemy2 import Geometry, WKTElement

dbname = 'map_the_vote'
username = 'docker'
password = 'docker'

engine = create_engine('postgres://%s:%s@localhost/%s'%(username,password,dbname))
print(engine.url)

if not database_exists(engine.url):
    print('Creating database')
    create_database(engine.url)
    print('Creating PostGID extension')
    sql_query = """
    CREATE EXTENSION postgis;
    """
    engine.execute(sql_query)
print(database_exists(engine.url))


print('Process Zip Codes (National File)')

print('project to geographic coordinates to match openaddresses')
zip_shapefile = gpd.read_file("data/Geography/Zip_Codes/tl_2017_us_zcta510.shp", encoding = 'utf-8')
zip_shapefile = zip_shapefile.to_crs({'init': 'epsg:4326'})
zip_shapefile.columns = map(str.lower, zip_shapefile.columns)

# I don't know why this is necessary, but it is
print('Create "geom" column')
zip_shapefile['geom'] = zip_shapefile['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))
print('Drop "geometry" column')
zip_shapefile.drop('geometry', 1, inplace=True)

# issue with multipolygons requires upload the schema first, alter the geom column, then upload the data
print('Create zip code table')
table_name = "zip5_us"
zip_shapefile.head(0).to_sql(table_name, engine, if_exists='replace', index=False, 
                                dtype={'geom': Geometry('Polygon', srid= 4326)})

print('Alter geom column')
sql_query = """
ALTER TABLE zip5_us ALTER COLUMN geom SET DATA TYPE geometry;
"""
engine.execute(sql_query)

print('Load zip code data')
table_name = "zip5_us"
CHUNK_SIZE = 1000

# TODO Should this be a 'replace' instead of 'append'?
zip_shapefile.head(CHUNK_SIZE).to_sql(table_name, engine, if_exists='append', index=False,
                                dtype={'geom': Geometry('Polygon', srid= 4326)})

for i in range(CHUNK_SIZE, zip_shapefile.shape[0]+CHUNK_SIZE, CHUNK_SIZE):
    zip_shapefile[i:(i + CHUNK_SIZE)].to_sql(table_name, engine, if_exists='append', index=False,
                                             dtype={'geom': Geometry('Polygon', srid= 4326)})
    print(i)


print ('Process Congressional Districts (National File)')

tl_2017_us_cd115 = gpd.read_file("data/Geography/Congressional_Districts/tl_2017_us_cd115.shp", encoding = 'utf-8')
print('project to geographic coordinates to match openaddresses')
tl_2017_us_cd115 = tl_2017_us_cd115.to_crs({'init': 'epsg:4326'})
tl_2017_us_cd115.columns = map(str.lower, tl_2017_us_cd115.columns)

# I don't know why this is necessary, but it is
print('Create "geom" column')
tl_2017_us_cd115['geom'] = tl_2017_us_cd115['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))
print('Drop "geometry" column')
tl_2017_us_cd115.drop('geometry', 1, inplace=True)

# issue with multipolygons requires upload the schema first, alter the geom column, then upload the data
print('Create congressional district table')
table_name = "us_congressional_districts"
tl_2017_us_cd115.head(0).to_sql(table_name, engine, if_exists='replace', index=False, 
                                dtype={'geom': Geometry('Polygon', srid= 4326)})

print('Alter geom column')
sql_query = """
ALTER TABLE us_congressional_districts ALTER COLUMN geom SET DATA TYPE geometry;
"""
engine.execute(sql_query)

tl_2017_us_cd115.to_sql(table_name, engine, if_exists='append', index=False,
                                dtype={'geom': Geometry('POLYGON', srid= 4326)})


print('Process VTDs (Texas)')

precinct_shapefile = gpd.read_file("data/Geography/VTDs/VTDs.shp", encoding = 'utf-8')
# SRID is 3081, checked in QGIS
print('project to geographic coordinates to match openaddresses')
precinct_shapefile = precinct_shapefile.to_crs({'init': 'epsg:4326'})
precinct_shapefile.columns = map(str.lower, precinct_shapefile.columns)

# I don't know why this is necessary, but it is
print('Create "geom" column')
precinct_shapefile['geom'] = precinct_shapefile['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))
print('Drop "geometry" column')
precinct_shapefile.drop('geometry', 1, inplace=True)

print('Create VTD table')
table_name = "vtds_tx"
precinct_shapefile.to_sql(table_name, engine, if_exists='replace', index=False, 
                                dtype={'geom': Geometry('POLYGON', srid= 4326)})
