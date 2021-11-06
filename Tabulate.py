import psycopg2
import gdal
from osgeo import ogr
from osgeo import osr
import os, subprocess

def load_shp(shape, srid, tablename, indexing):
    driver = ogr.GetDriverByName('ESRI Shapefile')
    start = time.time()
    if indexing:
        cmds = 'shp2pgsql -s ' + srid + ' -d -I "' + shape + '" ' + tablename + ' | psql '
    else:
        cmds = 'shp2pgsql -s ' + srid + ' -d "' + shape + '" ' + tablename + ' | psql '
    subprocess.call(cmds, shell=True)
    stop = time.time()
    print("Elapsed time (s): ", stop-start) 
    print(cmds)

#local settings
local_postgresql_path = r';C:\Program Files\PostgreSQL\13\bin'
host = 'localhost'
port = '5432'
user = 'my_username'
password = 'my_password'
database = 'my_database'
os.environ['PATH'] += local_postgresql_path
os.environ['PGHOST'] = host
os.environ['PGPORT'] = port
os.environ['PGUSER'] = user
os.environ['PGPASSWORD'] = password
os.environ['PGDATABASE'] = database

#point path to local county shapefile
shape_path = "/tl_2021_us_county.shp"
#loads county shapefile to PostGIS database
load_shp(shape_path, '4269', 'counties', True)
tract_path = "/TIGER_tracts/tl_2021_15_tract.shp"
load_shp(tract_path,'4269', 'tracts_hi', True)
block_path = "/TIGER_blocks/tl_2021_15_tabblock20.shp"
load_shp(block_path, '4269', 'blocks_hi', True)

#vectorizes landcover raster
#point path to local raster file
raster = gdal.Open("/hi_landcover_wimperv_9-30-08_se5.img")
outSpatialRef = osr.SpatialReference()
outSpatialRef.ImportFromEPSG(4269)
raster = gdal.Translate('/projected_lc.img', raster, outputSRS =  outSpatialRef)
srcband = raster.GetRasterBand(1)
prj = raster.GetProjection()
srs=osr.SpatialReference(wkt=prj)
dst_layername = 'polygonized_lc.shp'
drv = ogr.GetDriverByName("ESRI Shapefile")
dst_ds = drv.CreateDataSource(dst_layername)
dst_layer = dst_ds.CreateLayer(dst_layername, srs=srs)
fd = ogr.FieldDefn("DN", ogr.OFTInteger)
dst_layer.CreateField(fd)
dst_field = dst_layer.GetLayerDefn().GetFieldIndex("DN")
gdal.Polygonize(srcband, None, dst_layer, dst_field, [], callback=None)
print("Raster vectorized")

#point path to local vectorized landcover file
shape_path = "/polygonized_lc.shp"
load_shp(shape_path,'4269','landcover_hi', True)

#intersects county boundaries (Hawaii only) with vectorized landcover, 
#calculating intersected area in meters using HI state plane 3 projection
start = time.time()
connection = psycopg2.connect(database="tabulate",user="postgres", password="rostiBle#820")
cursor = connection.cursor()
cursor.execute("DROP TABLE IF EXISTS counties_hi")
cursor.execute("CREATE TABLE counties_hi AS SELECT * FROM counties WHERE statefp = '15';")
cursor.execute("DROP TABLE IF EXISTS co_lc_intersect")
cursor.execute("CREATE TABLE co_lc_intersect (countyfp varchar, gid varchar, dn integer, intersectarea_m2 double precision)")
cursor.execute("INSERT INTO co_lc_intersect (countyfp, gid, dn, intersectarea_m2) SELECT c.countyfp, h.gid, h.dn, ST_Area(ST_Transform(ST_Intersection(h.geom, c.geom), 26963)) FROM counties_hi c JOIN landcover_hi h on ST_Intersects(h.geom, c.geom)")
cursor.execute("DROP TABLE IF EXISTS total_developed_area")
cursor.execute("CREATE TABLE total_developed_area (statefp varchar, countyfp varchar, dn integer, totaldevarea_m2 double precision, countylandarea_m2 double precision, percentdeveloped double precision)")
cursor.execute("INSERT INTO total_developed_area (statefp, countyfp, dn, totaldevarea_m2, countylandarea_m2, percentdeveloped) SELECT c.statefp, i.countyfp, i.dn, sum(i.intersectarea_m2) as totaldevarea_m2, c.aland as countylandarea_m2, sum(i.intersectarea_m2)/c.aland*100 as percentdeveloped FROM co_lc_intersect i JOIN counties_hi c on c.countyfp = i.countyfp GROUP BY c.statefp, i.countyfp, i.dn, c.aland, c.geom HAVING dn in (21,22,23,24) ORDER BY countyfp, dn")
#aggregates developed land area types to find percentage of county land area that is developed
cursor.execute("DROP TABLE IF EXISTS county_developed_area")
cursor.execute("CREATE TABLE county_developed_area (statefp varchar, countyfp varchar, percentdeveloped double precision)")
cursor.execute("INSERT INTO county_developed_area (statefp, countyfp, percentdeveloped) SELECT t.statefp, t.countyfp, sum(t.percentdeveloped) as percentdeveloped FROM total_developed_area t GROUP BY t.statefp, t.countyfp")
connection.commit()
cursor.close()
connection.close()
stop = time.time()
print("Elapsed time (s): ", stop-start) 

#intersects tract boundaries (Hawaii only) with vectorized landcover, 
#calculating intersected area in meters using HI state plane 3 projection
start = time.time()
connection = psycopg2.connect(database="tabulate",user="postgres", password="rostiBle#820")
cursor = connection.cursor()
cursor.execute("DROP TABLE IF EXISTS ct_lc_intersect")
cursor.execute("CREATE TABLE ct_lc_intersect (geoid varchar, gid integer, dn integer, intersectarea_m2 double precision)")
cursor.execute("INSERT INTO ct_lc_intersect (geoid, gid, dn, intersectarea_m2) SELECT t.geoid, h.gid, h.dn, ST_Area(ST_Transform(ST_Intersection(h.geom, t.geom), 26963)) FROM tracts_hi t JOIN landcover_hi h on ST_Intersects(h.geom, t.geom)")
cursor.execute("DROP TABLE IF EXISTS total_developed_area_ct")
cursor.execute("CREATE TABLE total_developed_area_ct (geoid varchar, dn integer, totaldevarea_m2 double precision, tractlandarea_m2 double precision, percentdeveloped double precision)")
cursor.execute("INSERT INTO total_developed_area_ct (geoid, dn, totaldevarea_m2, tractlandarea_m2, percentdeveloped) SELECT t.geoid, i.dn, sum(i.intersectarea_m2) as totaldevarea_m2, t.aland as countylandarea_m2, sum(i.intersectarea_m2)/NULLIF(t.aland,0)*100 as percentdeveloped FROM ct_lc_intersect i JOIN tracts_HI t on t.geoid = i.geoid GROUP BY t.geoid, i.dn, t.aland, t.geom HAVING dn in (21,22,23,24) ORDER BY geoid, dn")
#aggregates developed land area types to find percentage of tract land area that is developed
cursor.execute("DROP TABLE IF EXISTS tract_developed_area")
cursor.execute("CREATE TABLE tract_developed_area (geoid varchar, percentdeveloped double precision)")
cursor.execute("INSERT INTO tract_developed_area (geoid, percentdeveloped) SELECT t.geoid, sum(t.percentdeveloped) as percentdeveloped FROM total_developed_area_ct t GROUP BY t.geoid")
connection.commit()
cursor.close()
connection.close()
stop = time.time()
print("Elapsed time (s): ", stop-start) 
