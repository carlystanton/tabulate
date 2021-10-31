import psycopg2
import gdal
from osgeo import ogr
from osgeo import osr
import os, subprocess

def load_shp(shape, srid, tablename):
    driver = ogr.GetDriverByName('ESRI Shapefile')
    cmds = 'shp2pgsql -s ' + srid + ' -d "' + shape + '" ' + tablename + ' | psql '
    subprocess.call(cmds, shell=True)
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
load_shp(shape_path, '4269', 'counties')

#vectorizes landcover raster
#point path to local raster file
raster = gdal.Open("/hi_landcover_wimperv_9-30-08_se5.img")
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

#projects vectorized raster into NAD83 for intersection with county boundaries
inSpatialRef = srs
outSpatialRef = osr.SpatialReference()
outSpatialRef.ImportFromEPSG(4269)
coordTrans = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)
inDataSet = drv.Open("/polygonized_lc.shp")
inLayer = inDataSet.GetLayer()
outputShapefile = "/projected_lc.shp"
if os.path.exists(outputShapefile):
    drv.DeleteDataSource(outputShapefile)
outDataSet = drv.CreateDataSource(outputShapefile)
outLayer = outDataSet.CreateLayer("projected_lc", outSpatialRef, geom_type=ogr.wkbMultiPolygon)

inLayerDefn = inLayer.GetLayerDefn()
for i in range(0, inLayerDefn.GetFieldCount()):
    fieldDefn = inLayerDefn.GetFieldDefn(i)
    outLayer.CreateField(fieldDefn)

outLayerDefn = outLayer.GetLayerDefn()
inFeature = inLayer.GetNextFeature()
while inFeature:
    geom = inFeature.GetGeometryRef()
    geom.Transform(coordTrans)
    outFeature = ogr.Feature(outLayerDefn)
    outFeature.SetGeometry(geom)
    for i in range(0, outLayerDefn.GetFieldCount()):
        outFeature.SetField(outLayerDefn.GetFieldDefn(i).GetNameRef(), inFeature.GetField(i))
    outLayer.CreateFeature(outFeature)
    outFeature = None
    inFeature = inLayer.GetNextFeature()

inDataSet = None
outDataSet = None

#point path to local vectorized landcover file
shape_path = "/projected_lc.shp"
load_shp(shape_path,'4269','landcover_hi')

#intersects county boundaries (Hawaii only) with vectorized landcover, 
#calculating intersected area in meters using HI state plane 3 projection
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