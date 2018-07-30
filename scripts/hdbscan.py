
# coding: utf-8

# In[1]:


import hdbscan
import psycopg2
import matplotlib

import pandas as pd
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from hdbscan import HDBSCAN
from sklearn.cluster import DBSCAN
from sklearn import metrics
from sklearn.datasets.samples_generator import make_blobs
from sklearn.preprocessing import StandardScaler

from geoalchemy2 import Geometry, WKTElement
from shapely.geometry import Point


# In[2]:


plt.rcParams['figure.figsize'] = [10, 10]


# In[3]:


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


# In[4]:


# Connect to make queries using psycopg2
con = None
con = psycopg2.connect(host='localhost', database = dbname, user = username, password=password)


# In[5]:


sql_query = """
SELECT * FROM final_addresses_not_joined_vtd;
"""
pts = gpd.GeoDataFrame.from_postgis(sql_query, con, geom_col='geom')


# In[ ]:


X = pts[["oa_lon","oa_lat"]].values


# In[7]:


# Compute DBSCAN
hdb = HDBSCAN(min_cluster_size=10).fit(X)
hdb_labels = hdb.labels_


# In[8]:


pts['hdb_labels'] = hdb_labels


# In[9]:


len(np.unique(hdb_labels))


# In[10]:


x = pts['hdb_labels'].values
no_cluster = x[x==-1]
len(no_cluster)


# In[11]:


x = x[x>-1]
y = np.bincount(x)


# In[12]:


max(y)


# In[13]:


sum(y > 80)


# In[14]:


np.mean(y)


# In[15]:


np.std(y)


# In[16]:


plt.hist(y, bins='auto') 
plt.xlim((0, 200)) 
matplotlib.rcParams.update({'font.size': 20})
plt.show()


# In[17]:


pts.to_file(driver = 'ESRI Shapefile', filename= "hdbscan_labels.shp")


# In[18]:


pts = gpd.read_file("/Users/codyschank/Dropbox/Insight/hdbscan_labels.shp", encoding = 'utf-8')


# In[19]:


pts.columns


# In[20]:


pts['geom'] = pts['geometry'].apply(lambda x: WKTElement(x.wkt, srid=3081))
pts.drop('geometry', 1, inplace=True)


# In[21]:


table_name = "final_addresses_not_joined_hdbscan"
pts.head(1000).to_sql(table_name, engine, if_exists='replace', index=False, 
                        dtype={'geom': Geometry('POINT', srid= 3081)})


# In[22]:


chunk_size = 1000
for i in range(1000, pts.shape[0]+chunk_size, chunk_size):
    print(i)
    pts[i:(i+chunk_size)].to_sql(table_name, engine, if_exists='append', index=False, 
                                    dtype={'geom': Geometry('POINT', srid= 3081)})


# In[23]:


sql_query = """
CREATE INDEX final_addresses_not_joined_hdbscan_gix ON final_addresses_not_joined_hdbscan USING GIST (geom);
"""
engine.execute(sql_query)

