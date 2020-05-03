#!/usr/bin/env python
# coding: utf-8

# In[1]:


def createIndex(geojson):
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(geojson).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)


# In[2]:


def findZone(p, index, zones):
    for idx in index.intersection((p.x, p.y, p.x, p.y)):
        if zones.geometry[idx].contains(p):
            return idx
    return None


# In[3]:


def processTrips(pid, records):
    import csv
    import pyproj
    import shapely.geometry as geom
    
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    index, zones = createIndex('neighborhoods.geojson')    
    
    if pid==0:
        next(records)
    reader = csv.reader(records)
    
    for row in reader:
        if len(row) == 18:
            try:
                p_start = geom.Point(proj(float(row[5]), float(row[6])))
                p_des = geom.Point(proj(float(row[9]), float(row[10])))
                start = findZone(p_start, index, zones)
                des = findZone(p_des, index, zones)
            except ValueError:
                continue
            if (start != None) and (des != None):
                yield ((zones['borough'][start], zones['neighborhood'][des]),1)


# In[4]:


def toCSV(data):
    return ','.join(str(d) for d in data)


# In[5]:


from pyspark import SparkContext
from heapq import nlargest
import sys
import os

if __name__ == '__main__':
    input_file = sys.argv[1]
    output_file = sys.argv[2]

    rdd = SparkContext().textFile(input_file)
    rdd.mapPartitionsWithIndex(processTrips)         .reduceByKey(lambda x,y: x+y)         .map(lambda x: (x[0][0],x[0][1],x[1]))         .groupBy(lambda x: x[0])         .flatMap(lambda y: nlargest(3, y[1], key = lambda x: x[2]))         .map(lambda x: (x[0],(x[1],x[2])))         .reduceByKey(lambda x,y: x+y)         .sortByKey()         .map(lambda x: ((x[0],)+x[1]))         .map(toCSV)         .saveAsTextFile(output_file)


# In[ ]:




