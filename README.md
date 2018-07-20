# insight_map_the_vote

"Map the Vote" is a project to map unregistered voters in Texas. Currently we are focusing on single-family homes that have no registered voters, as determined using voter files from three congressional districts (TX-10, TX-21, and TX-25).

The Jupyter Notebooks contained in this repo were developed for uploading and processing data in a Postgres/PostGIS database to produce the final table, which is used in the web app found at http://mapthevote.io/. The repo for that web app can be found at https://github.com/codyschank/application

The processing should proceed in this order:
1) Upload geographies
  -National zip code and congressional district shapefiles can be found here: https://www.census.gov/geo/maps-data/data/tiger.html
  -VTD (voter tabulation district) file for Texas can be found here: ftp://ftpgis1.tlc.state.tx.us/2011_Redistricting_Data/VTDs/Geography/
2) Upload voter file
  -These files are not freely available, but can be obtained by working with political campaigns who have access to this data
3) Upload open addresses
  -Available here https://openaddresses.io/
4) Joins, fuzzy matching, etc.
5) HDBSCAN
6) Heat Map (optional, not currently on the web app)


Feel free to email me with any questions codyschank@gmail.com
