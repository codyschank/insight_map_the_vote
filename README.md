# insight_map_the_vote

"Map the Vote" is a project to map unregistered voters in Texas. Currently we are focusing on single-family homes that have no registered voters, as determined using voter files from three congressional districts (TX-10, TX-21, and TX-25).

The Jupyter Notebooks contained in this repo were developed for uploading and processing data in a Postgres/PostGIS database to produce the final table, which is used in the web app found at http://mapthevote.io/. The repo for that web app can be found at https://github.com/codyschank/application.

The processing should proceed in this order:

1) Upload geographies: National zip code and congressional district shapefiles (https://www.census.gov/geo/maps-data/data/tiger.html). VTD (voter tabulation district) file for Texas (ftp://ftpgis1.tlc.state.tx.us/2011_Redistricting_Data/VTDs/Geography/).

2) Upload voter file: These files are not freely available, but can be obtained by working with political campaigns who have access to this data.

3) Upload open addresses (https://openaddresses.io/)

4) Joins, fuzzy matching, etc.: requires an API key for SmartyStreets (https://smartystreets.com/).

5) HDBSCAN.

6) Heat Map (optional, not currently on the web app).


Feel free to email me with any questions codyschank@gmail.com


## Installation

### Using Conda environment:

`conda create -n map_the_vote python=3.6.6`

`source activate map_the_vote`

### Install packages

`pip install -r requirements.txt`

Depending on the state of your system, you may also need to do one or more of the following:

`brew install geos`

`conda install cython`

`conda install numpy scipy`

`conda install scikit-learn`

`conda install -c conda-forge hdbscan`

### Install Docker Requirements

`https://docs.docker.com/docker-for-mac/install/`

`https://docs.docker.com/docker-for-windows/install/`

Run `docker-compose up` in the `insight_map_the_vote` directory.


### To update dependencies file, pipe the output of pip freeze to the requirements file

`pip freeze > requirements.txt`

