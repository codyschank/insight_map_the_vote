import psycopg2
import numpy
import difflib

import pandas as pd
import geopandas as gpd

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry import Point

from secrets import *

pd.options.mode.chained_assignment = None

def get_rdi(street_address, zip_code):
    auth_id = AUTH["smarty_auth_id"]
    auth_token = AUTH["smarty_auth_token"]
    credentials = StaticCredentials(auth_id, auth_token)
    client = ClientBuilder(credentials).build_us_street_api_client()

    lookup = Lookup()
    lookup.street = street_address
    lookup.zipcode = zip_code

    try:
        client.send_lookup(lookup)
    except exceptions.SmartyException as err:
        print(err)
        return 'invalid'

    result = lookup.result

    if not result:
        #print("No candidates. This means the address is not valid.")
        return('invalid')

    return(result[0])

dbname = 'map_the_vote'
username = 'docker'
password = 'docker'

engine = create_engine('postgres://%s:%s@localhost/%s'%(username,password,dbname))
print(engine.url)

if not database_exists(engine.url):
    create_database(engine.url)
print(database_exists(engine.url))

con = None
con = psycopg2.connect(host='localhost', database = dbname, user = username, password=password)

# ## Initial Joins

# intersect open addresses with districts where I have voter files
# check that both of the geom fields have indexes, if not create them to make this faster
sql_query = """
CREATE TABLE select_all_addresses AS
SELECT b.* FROM us_congressional_districts a, addresses_table_tx_no_dupes b 
    WHERE ST_Intersects(a.geom,b.geom) AND a.geoid IN ('4821','4825','4810');
"""
engine.execute(sql_query)

sql_query = """
CREATE INDEX oa_street_address_idx ON select_all_addresses (oa_street_address);
"""
engine.execute(sql_query)

# Join open addresses to voter file
# use upper for oa_address, vf_address already capitalized
sql_query = """
CREATE TABLE voters_join AS
SELECT a.geom, a.oa_lon, a.oa_lat, a.oa_number, a.oa_postcode, a.oa_street_address, a.oa_street, b.* FROM select_all_addresses a LEFT JOIN voter_file_all b ON upper(a.oa_street_address) = b.vf_street_address;
"""
engine.execute(sql_query)

# create table of voters not joined to open addresses
sql_query = """
CREATE TABLE addresses_not_joined AS
SELECT geom, oa_lon, oa_lat, oa_number, oa_postcode, oa_street_address, oa_street FROM voters_join WHERE vf_voter_file_vanid IS NULL;
"""
engine.execute(sql_query)

# voter_join is now only the voters and addresses joined correctly
sql_query = """
DELETE FROM voters_join
WHERE vf_voter_file_vanid IS NULL;
"""
engine.execute(sql_query)

# create table for voters not joined
sql_query = """
CREATE TABLE voters_not_joined AS
SELECT a.* FROM voter_file_all a LEFT JOIN select_all_addresses b ON a.vf_street_address = upper(b.oa_street_address)
WHERE b.geom IS NULL;
"""
engine.execute(sql_query)

# ## Fuzzy Matching

sql_query = """
SELECT * FROM addresses_not_joined;
"""
addresses_not_joined = pd.read_sql_query(sql_query,con)

sql_query = """
SELECT * FROM voters_not_joined;
"""
voters_not_joined = pd.read_sql_query(sql_query,con)

unique_street_address = voters_not_joined[['vf_streetno','vf_zip5','vf_street_address']].drop_duplicates()

unique_street_address['fuzzy_match'] = ''
unique_street_address['fuzzy_score'] = 0

for i in range(0,unique_street_address.shape[0]):
    street_no = unique_street_address.iloc[i]['vf_streetno']
    zip_code = unique_street_address.iloc[i]['vf_zip5']
    street_address = unique_street_address.iloc[i]['vf_street_address']
    potential_matches = addresses_not_joined.loc[(addresses_not_joined.oa_number == street_no) & (addresses_not_joined.oa_postcode == zip_code)]['oa_street_address'].values
    if (len(potential_matches)>0):
        closest_match = difflib.get_close_matches(street_address, potential_matches, n=1)
        if(len(closest_match)>0):
            match_score = difflib.SequenceMatcher(None, street_address, closest_match[0]).ratio()
            unique_street_address.iloc[i,unique_street_address.columns.get_loc('fuzzy_match')] = closest_match
            unique_street_address.iloc[i,unique_street_address.columns.get_loc('fuzzy_score')] = match_score
    if((i % 100) == 0):
        print(i)

table_name = "voter_addresses_fuzzy_match"
#write first 1000 rows to database
unique_street_address.head(1000).to_sql(table_name, engine, if_exists='replace')

chunk_size = 1000
for i in range(1000, unique_street_address.shape[0]+chunk_size, chunk_size):
    print(i)
    unique_street_address[i:(i+chunk_size)].to_sql(table_name, engine, if_exists='append')

sql_query = """
CREATE TABLE voters_fuzzy_join AS
SELECT a.*, b.fuzzy_match, b.fuzzy_score FROM voters_not_joined a LEFT JOIN voter_addresses_fuzzy_match b ON a.vf_street_address = b.vf_street_address;
"""
engine.execute(sql_query)

sql_query = """
CREATE TABLE oa_fuzzy_join AS
SELECT a.geom, a.oa_lon, a.oa_lat, a.oa_number, a.oa_postcode, a.oa_street_address, a.oa_street, b.* FROM addresses_not_joined a LEFT JOIN voters_fuzzy_join b ON a.oa_street_address = b.fuzzy_match;
"""
engine.execute(sql_query)

# create table of voters not joined to open addresses
sql_query = """
CREATE TABLE addresses_still_not_joined AS
SELECT geom, oa_lon, oa_lat, oa_number, oa_postcode, oa_street_address, oa_street FROM oa_fuzzy_join 
WHERE vf_voter_file_vanid IS NULL OR fuzzy_score < 0.867;
"""
engine.execute(sql_query)

# oa_fuzzy_join is now only the voters and addresses fuzzy matched correctly
sql_query = """
DELETE FROM oa_fuzzy_join
WHERE vf_voter_file_vanid IS NULL OR fuzzy_score < 0.867;
"""
engine.execute(sql_query)

sql_query = """
CREATE TABLE voters_still_not_joined AS
SELECT * FROM voters_fuzzy_join 
WHERE fuzzy_score < 0.867;
"""
engine.execute(sql_query)

# ## Smarty Streets RDI Check

# If smarty_streets_rdi_check_new doesn't exist
# sql_query = """
# SELECT DISTINCT(a.oa_street_address) FROM addresses_not_joined;
# """
# addresses_to_check = pd.read_sql_query(sql_query,con)

# get addresses to check that have not already been checked
sql_query = """
SELECT DISTINCT(a.oa_street_address) FROM addresses_not_joined a 
LEFT JOIN smarty_streets_rdi_check_new b
ON a.oa_street_address = b.oa_street_address
WHERE b.Index IS NULL;
"""
addresses_to_check = pd.read_sql_query(sql_query,con)

addresses_checked = pd.DataFrame(index=range(0, addresses_to_check.shape[0],1),columns=['oa_street_address','oa_street_address_no_zip','zip_code','residential','vacant','active','ss_lon','ss_lat'],dtype='object')

for i in range(0,addresses_to_check.shape[0],1):
    oa_street_address = addresses_to_check.iloc[i].oa_street_address
    address_no_zip = oa_street_address[0:-6]
    zipcode = oa_street_address[-5:]
    addresses_checked.loc[i]['oa_street_address'] = oa_street_address
    addresses_checked.loc[i]['oa_street_address_no_zip'] = address_no_zip
    addresses_checked.loc[i]['zipcode'] = zipcode
    result = get_rdi(address_no_zip,zipcode)
    if result == 'invalid':
        addresses_checked.loc[i]['residential'] = 'invalid'
    else:
        addresses_checked.loc[i]['residential'] = result.metadata.rdi
        addresses_checked.loc[i]['vacant'] = result.analysis.vacant
        addresses_checked.loc[i]['active'] = result.analysis.active
        addresses_checked.loc[i]['ss_lon'] = result.metadata.longitude
        addresses_checked.loc[i]['ss_lat'] = result.metadata.latitude
    if(i%100 == 0):
        print(i)

# run this the first time the table is created
# table_name = "smarty_streets_rdi_check_new"
# write first 1000 rows to database
# addresses_checked.head(1000).to_sql(table_name, engine, if_exists='replace')
# chunk_size = 1000
# for i in range(1000, addresses_checked.shape[0]+chunk_size, chunk_size):
#    addresses_checked[i:(i+chunk_size)].to_sql(table_name, engine, if_exists='append')
#    print(i)

# Insert into table if already exists
addresses_checked.head(1000).to_sql("smarty_streets_rdi_check_new", engine, if_exists='append')
if(addresses_checked.shape[0]>1000):
    chunk_size = 1000
    for i in range(1000, addresses_checked.shape[0]+chunk_size, chunk_size):
        print(i)
        addresses_checked[i:(i+chunk_size)].to_sql("smarty_streets_rdi_check_new", engine, if_exists='append')


# ## Smarty Streets Geocode Voters

# If smarty_streets_geocode doesn't exist
# sql_query = """
# SELECT DISTINCT(a.vf_street_address) FROM voters_still_not_joined;
# """
# voters_to_geocode = pd.read_sql_query(sql_query,con)

# get voters to geocode that have not already been geocoded
sql_query = """
SELECT DISTINCT(a.vf_street_address) FROM voters_still_not_joined a 
LEFT JOIN smarty_streets_geocode b
ON a.vf_street_address = b.vf_street_address
WHERE b.vf_street_address IS NULL;
"""
voters_to_geocode = pd.read_sql_query(sql_query,con)


voters_geocoded = pd.DataFrame(index=range(0, voters_to_geocode.shape[0],1),columns=['vf_street_address','vf_street_address_no_zip','ss_lon','ss_lat','residential','vacant'],dtype='object')

for i in range(0,voters_to_geocode.shape[0],1):
    vf_street_address = voters_to_geocode.iloc[i].vf_street_address
    address_no_zip = vf_street_address[0:-6]
    zipcode = vf_street_address[-5:]
    voters_geocoded.loc[i]['vf_street_address'] = vf_street_address
    voters_geocoded.loc[i]['vf_street_address_no_zip'] = address_no_zip
    result = get_rdi(address_no_zip,zipcode)
    if result == 'invalid':
        voters_geocoded.loc[i]['residential'] = 'invalid'
    else:
        voters_geocoded.loc[i]['residential'] = result.metadata.rdi
        voters_geocoded.loc[i]['vacant'] = result.analysis.vacant
        voters_geocoded.loc[i]['active'] = result.analysis.active
        voters_geocoded.loc[i]['ss_lon'] = result.metadata.longitude
        voters_geocoded.loc[i]['ss_lat'] = result.metadata.latitude
    if(i%100 == 0):
        print(i)

# Insert new rows into table
voters_geocoded.head(1000).to_sql("smarty_streets_geocode", engine, if_exists='append')
if(voters_geocoded.shape[0]>1000):
    chunk_size = 1000
    for i in range(1000, voters_geocoded.shape[0]+chunk_size, chunk_size):
        print(i)
        voters_geocoded[i:(i+chunk_size)].to_sql("smarty_streets_geocode", engine, if_exists='append')

# if table is created new
# sql_query = """
# SELECT AddGeometryColumn('smarty_streets_geocode','geom',4326,'POINT',2);
# """
# engine.execute(sql_query)

sql_query = """
UPDATE smarty_streets_geocode SET geom = ST_SetSRID(ST_MakePoint(ss_lon::double precision, ss_lat::double precision), 4326);
"""
engine.execute(sql_query)


# ## Final Joins and Processing

# create table voters_not_joined_geocoded by joining geocoded voters not joined with original table that identified them, tx25_join_check2
sql_query = """
CREATE TABLE voters_not_joined_geocoded AS
SELECT a.*, b.geom FROM voters_still_not_joined a LEFT JOIN smarty_streets_geocode b ON a.vf_street_address = b.vf_street_address;
"""
engine.execute(sql_query)

sql_query = """
SELECT COUNT(*) FROM voters_not_joined_geocoded WHERE geom IS NULL;
"""
pd.read_sql_query(sql_query,con)

# now append voters_not_joined_geocoded to voters_join
# I should reconsider doing an insert here, since it means I will need to redo the original join if I want to change anything
# maybe I can add a field that tells how the record was geocoded, althought, that should be recorded by whether the row has an oa_address
sql_query = """
INSERT INTO voters_join (geom, vf_voter_file_vanid, vf_sex, vf_age, vf_street_address, vf_multi_unit, vf_cntyvtd)
SELECT geom, vf_voter_file_vanid, vf_sex, vf_age, vf_street_address, vf_multi_unit, vf_cntyvtd
FROM voters_not_joined_geocoded;
"""
engine.execute(sql_query)

# now append oa_fuzzy_join to voters_join
# I should reconsider doing an insert here, since it means I will need to redo the original join if I want to change anything
# maybe I can add a field that tells how the record was geocoded, althought, that should be recorded by whether the row has an oa_address
sql_query = """
INSERT INTO voters_join (geom, vf_voter_file_vanid, vf_sex, vf_age, vf_street_address, vf_multi_unit, vf_cntyvtd)
SELECT geom, vf_voter_file_vanid, vf_sex, vf_age, vf_street_address, vf_multi_unit, vf_cntyvtd
FROM oa_fuzzy_join;
"""
engine.execute(sql_query)

sql_query = """
SELECT COUNT(*) FROM voters_join;
"""
pd.read_sql_query(sql_query,con)

sql_query = """
CREATE TABLE voters_not_joined_geocoded_3081 AS
    SELECT * FROM voters_not_joined_geocoded;
"""
engine.execute(sql_query)

sql_query = """
ALTER TABLE voters_not_joined_geocoded_3081 
   ALTER COLUMN geom 
   TYPE Geometry(Point, 3081) 
   USING ST_Transform(geom, 3081);
"""
engine.execute(sql_query)

sql_query = """
CREATE INDEX voters_not_joined_geocoded_3081_gix ON voters_not_joined_geocoded_3081 USING GIST (geom);
"""
engine.execute(sql_query)

# Copy and project tables that I need for DWithin, calculate indices
# I could have used this projection from the beginning
sql_query = """
CREATE TABLE addresses_still_not_joined_3081 AS
    SELECT * FROM addresses_still_not_joined;
"""
engine.execute(sql_query)

sql_query = """
ALTER TABLE addresses_still_not_joined_3081 
   ALTER COLUMN geom 
   TYPE Geometry(Point, 3081) 
   USING ST_Transform(geom, 3081);
"""
engine.execute(sql_query)

sql_query = """
CREATE INDEX addresses_still_not_joined_3081_gix ON addresses_still_not_joined_3081 USING GIST (geom);
"""
engine.execute(sql_query)

# this table might come in handy, but right now don't have a need for it
sql_query = """
CREATE TABLE voters_join_3081 AS
    SELECT * FROM voters_join;
"""
engine.execute(sql_query)

sql_query = """
ALTER TABLE voters_join_3081 
   ALTER COLUMN geom 
   TYPE Geometry(Point, 3081) 
   USING ST_Transform(geom, 3081);
"""
engine.execute(sql_query)

sql_query = """
CREATE INDEX voters_join_3081_gix ON voters_join_3081 USING GIST (geom);
"""
engine.execute(sql_query)

sql_query = """
ALTER TABLE addresses_still_not_joined_3081
ADD COLUMN join_mask integer;
"""
engine.execute(sql_query)

sql_query = """
UPDATE addresses_still_not_joined_3081
SET "join_mask" = 0;
"""
engine.execute(sql_query)

sql_query = """
UPDATE addresses_still_not_joined_3081 dst
SET "join_mask" = 1
FROM voters_join_3081 src
WHERE ST_DWITHIN(src.geom,dst.geom,10);
"""
engine.execute(sql_query)

sql_query = """
SELECT COUNT(*) FROM addresses_still_not_joined_3081 WHERE join_mask = 1;
"""
pd.read_sql_query(sql_query,con)

sql_query = """
CREATE TABLE final_addresses_not_joined AS 
SELECT a.*, b.residential, b.vacant, b.active FROM addresses_still_not_joined_3081 a LEFT JOIN smarty_streets_rdi_check_new b ON a.oa_street_address = b.oa_street_address;
"""
engine.execute(sql_query)

sql_query = """
DELETE FROM final_addresses_not_joined
WHERE residential != 'Residential' OR join_mask = 1 OR active = 'N' OR vacant = 'Y';
"""
engine.execute(sql_query)

sql_query = """
SELECT COUNT(*) FROM final_addresses_not_joined;
"""
pd.read_sql_query(sql_query,con)

sql_query = """
CREATE INDEX final_addresses_not_joined_gix ON final_addresses_not_joined USING GIST (geom);
"""
engine.execute(sql_query)

# project vtds_tx to 3081

sql_query = """
CREATE TABLE vtds_tx_3081 AS
    SELECT * FROM vtds_tx;
"""
engine.execute(sql_query)

sql_query = """
ALTER TABLE vtds_tx_3081 
   ALTER COLUMN geom 
   TYPE Geometry(Polygon, 3081) 
   USING ST_Transform(geom, 3081);
"""
engine.execute(sql_query)

sql_query = """
CREATE INDEX vtds_tx_3081_gix ON vtds_tx_3081 USING GIST (geom);
"""
engine.execute(sql_query)

# spatial join the cntyvtd field to final_addresses_not_joined
sql_query = """
CREATE TABLE final_addresses_not_joined_vtd AS
SELECT p1.*, p2.cntyvtd FROM final_addresses_not_joined p1, vtds_tx_3081 p2 WHERE ST_WITHIN(p1.geom, p2.geom)
"""
engine.execute(sql_query)

sql_query = """
SELECT COUNT(DISTINCT(vf_street_address)) FROM voters_join_3081;
"""
pd.read_sql_query(sql_query,con)
