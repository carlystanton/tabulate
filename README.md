# tabulate
Land cover tabulation for this project uses an open-source GIS database called PostGIS. Running tabulate.py requires installing
PostgreSQL with PostGIS and creating a PostgreSQL database and activating the PostGIS extension. Instructions can be found here:
https://postgis.net/install/ 

Running tabulate.py also requires installation of the following Python libraries:

psycopg2

gdal

osgeo

Local database settings must be added to the .py file for the following variables:

local_postgresql_path

host

port

user

password

database


tabulate.py works with these test files:

tl_2021_us_county.shp from https://www.census.gov/cgi-bin/geo/shapefiles/index.php (Year: 2021, Layer Type: Counties (and equivalent))
tl_2021_15_tract.shp from https://www.census.gov/cgi-bin/geo/shapefiles/index.php (Year: 2021, Layer Type: Census Tracts, State: Hawaii))
tl_2021_15_tabblock20.shp from https://www.census.gov/cgi-bin/geo/shapefiles/index.php (Year: 2021, Layer Type: Blocks, State: Hawaii))

hi_landcover_wimperv_9-30-08_se5.img from https://www.mrlc.gov/data?f%5B0%5D=region%3Aislands%20%26%20territories (Download "NLCD 2001 Land Cover (HAWAII)")

Paths to local files may need to be changed.

tabulate.py completes these steps:
1) loads the shapefiles to the PostGIS database using the shp2pgsql command line function
2) vectorizes the landcover raster using gdal's Polygonize function
3) projects the raster for intersection with boundaries 
4) loads the vectorized landcover shapefile using the shp2pgsql command line function
5) loads a subset of counties into a table (Hawaii only) with PostgreSQL through psycopg2
6) intersects all boundaries with the vectorized raster for all land cover classes with PostgreSQL through psycopg2
7) aggregates all developed land area classes into a table with the percentage of administrative land area that is developed for
	each area with PostgreSQL through psycopg2
8) sums percentage developed area for each area into a final results table with PostgreSQL through psycopg2
