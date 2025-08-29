# This script is for calculating the total road length in Chinese prefecture cities
# using the OpenStreetMap attic data.
# Limitations: Due to API capacity limitation, I only count the primary level roads
#               and use the city bbox rather than polygon.
#
#%%
import geopandas as gpd
import osmnx as ox
import requests
from shapely.geometry import LineString
from shapely import bounds
import pandas as pd
from pypinyin import lazy_pinyin
import time
import matplotlib as plt


#%% Load the list of cities and get the search key for OSM
city_shapes = gpd.read_file("C:/Users/yaoan/OneDrive/Documents/PhD Research/全国100万基础地理数据2017/全国100万基础地理数据2017/地级行政区.shp")
prv_shapes = gpd.read_file("C:/Users/yaoan/OneDrive/Documents/PhD Research/全国100万基础地理数据2017/全国100万基础地理数据2017/省级行政区.shp")
cities = city_shapes[['NAME', 'ENTIID']].drop_duplicates()
cities['adcode'] = cities['ENTIID'].str[4:8]
cities['prv_code'] = cities['ENTIID'].str[4:6]
cities = cities.drop('ENTIID', axis=1)

#%% Get the boundary of cities
city_shapes = city_shapes.astype({'Shape_Area': float}, copy = False)
city_shapes = city_shapes.sort_values(by = ['ENTIID', 'Shape_Area'], ascending=[True, False]).drop_duplicates(subset=['ENTIID'], keep='first')
city_shapes['minx'] = city_shapes.geometry.bounds.minx
city_shapes['miny'] = city_shapes.geometry.bounds.miny
city_shapes['maxx'] = city_shapes.geometry.bounds.maxx
city_shapes['maxy'] = city_shapes.geometry.bounds.maxy

#%%

# Overpass API with attic support, not using omsnx because it only supports real-time map data
url = "https://overpass-api.de/api/interpreter"

road_length = pd.DataFrame(columns=['year', 'adcode', 'city', 'road_length'])

#%%
for i in range(len(city_shapes)):


    # Define the bbox of cities. I don't use the polygon because it is too complicated and the API run out of memory to process
    boundary = ", ".join((str(city_shapes.iloc[i].miny), str(city_shapes.iloc[i].minx), str(city_shapes.iloc[i].maxy), str(city_shapes.iloc[i].maxx)))

    # Define the query
    query = f"""
    [out:json][timeout:150][date:"2020-01-01T00:00:00Z"];
    (
      way["highway"="primary"]({boundary});
    );
    out body;
    >;
    out skel qt;
    """

    # Retrieve the road data
    response = requests.post(url, data={"data": query})
    if response.status_code == 200:
        data = response.json()
        # Helper to build a dictionary of nodes
        elements = data['elements']
        nodes = {el['id']: (el['lon'], el['lat']) for el in elements if el['type'] == 'node'}

        # Build roads as LineStrings
        features = []
        for el in elements:
            if el['type'] == 'way' and 'nodes' in el:
                coords = [nodes[node_id] for node_id in el['nodes'] if node_id in nodes]
                if len(coords) >= 2:
                    line = LineString(coords)
                    tags = el.get('tags', {})
                    tags['geometry'] = line
                    features.append(tags)

        # Create GeoDataFrame
        roads = gpd.GeoDataFrame(features, geometry='geometry', crs="EPSG:4326")


        # Print total length (in meters, after projecting)
        roads = roads.to_crs(roads.estimate_utm_crs()) # Use a local (UTM-based) projected CRS in meters
        road_length.loc[len(road_length)] = [2020, city_shapes.iloc[i, 2][4:8], city_shapes.iloc[i, 5], roads.length.sum()]


    else:
        road_length.loc[len(road_length)] = [2020, city_shapes.iloc[i, 2][4:8], city_shapes.iloc[i, 5], None]
        continue
    time.sleep(3)

#%%
road_length.to_excel("dataprocessing/output/road_length2020.xlsx", index = False)
#%%

# #%% Getting the English names of the cities and provinces for searching in OSM
# provinces = prv_shapes[['NAME', 'ENTIID']].drop_duplicates()
# provinces['prv_code'] = provinces['ENTIID'].str[4:6]
# provinces = provinces[provinces['NAME']!='中朝共有']
# provinces = provinces[provinces['NAME']!=None]
# provinces = provinces.drop('ENTIID', axis=1)
# cities = cities.merge(provinces, how = 'left', on = 'prv_code', validate='m:1')
# cities = cities[cities['prv_code']!=None]
# cities = cities.rename(columns={'NAME_y': 'prv', 'NAME_x':'city'})
# cities['prv_short'] = cities['prv'].str[0:2]
# cities['city_short'] = cities['city'].str[0:2]
#
# prv_eng_names= pd.read_csv("C:/Users/yaoan/OneDrive/Documents/PhD Research/全国100万基础地理数据2017/provinces.csv")
# prv_eng_names['prv_short'] = prv_eng_names['prv'].str[0:2]
#
#
# cities = cities.merge(prv_eng_names, how = 'left', on = 'prv_short', validate='m:1')
#
# city_eng_names= pd.read_csv("C:/Users/yaoan/OneDrive/Documents/PhD Research/全国100万基础地理数据2017/chinese cities.csv")
# cities = cities.merge(city_eng_names, how = 'left', on = ['city'], validate='m:1')
#
# cities = cities[cities['city']!= "省直辖县级行政区"]
# cities = cities[cities['city']!= None]
# cities = cities.drop(10)
#
# py = cities[cities['city_eng'].isna()].copy()
# py['city_short']=  py['city_short'].astype("string")
# py['pinyin'] = py['city_short'].apply(lambda x: ''.join(lazy_pinyin(x)))
#
# cities = cities.merge(py[['adcode', 'pinyin']], how = 'left', on = 'adcode', validate = 'm:m')
# cities['city_eng'] = cities['city_eng'].fillna(cities['pinyin'])
# cities = cities[['adcode', 'city', 'city_short', 'city_eng', 'prv_short', 'prv_eng_x']]
# cities = cities.drop_duplicates(subset=['adcode'])
# cities['city_eng'] = cities['city_eng'].astype("string")
# cities['prv_eng'] = cities['prv_eng_x'].astype("string")
#
# cities = cities[~cities['city_eng'].isna()& ~cities['prv_eng'].isna()]
#
#
#
# #%%
# for i in range(11, len(cities)):
#
#     # Get the boundary of cities
#     gdf = ox.geocode_to_gdf(cities.iloc[i, 3] + ", " +  cities.iloc[i, 6] + ", china")
#
#
#     # Define the bbox of cities. I don't use the polygon because it is too complicated and the API run out of memory to process
#     boundary = ", ".join((str(gdf.iloc[0].bbox_south), str(gdf.iloc[0].bbox_west), str(gdf.iloc[0].bbox_north), str(gdf.iloc[0].bbox_east)))
#
#     # Define the query
#     query2015 = f"""
#     [out:json][timeout:150][date:"2015-01-01T00:00:00Z"];
#     (
#       way["highway"="primary"]({boundary});
#     );
#     out body;
#     >;
#     out skel qt;
#     """
#
#     # Retrieve the road data
#     response = requests.post(url, data={"data": query2015})
#     if response.status_code == 200:
#         data = response.json()
#     else:
#         raise Exception(f"Error: {response.status_code} - {response.text}")
#
#     # Helper to build a dictionary of nodes
#     elements = data['elements']
#     nodes = {el['id']: (el['lon'], el['lat']) for el in elements if el['type'] == 'node'}
#
#     # Build roads as LineStrings
#     features = []
#     for el in elements:
#         if el['type'] == 'way' and 'nodes' in el:
#             coords = [nodes[node_id] for node_id in el['nodes'] if node_id in nodes]
#             if len(coords) >= 2:
#                 line = LineString(coords)
#                 tags = el.get('tags', {})
#                 tags['geometry'] = line
#                 features.append(tags)
#
#     # Create GeoDataFrame
#     roads = gpd.GeoDataFrame(features, geometry='geometry', crs="EPSG:4326")
#
#
#     # Print total length (in meters, after projecting)
#     roads = roads.to_crs(gdf.estimate_utm_crs()) # Use a local (UTM-based) projected CRS in meters
#     road_length.loc[len(road_length)] = [2015, cities.iloc[i, 0], cities.iloc[i, 1], roads.length.sum()]
#     time.sleep(3)
#
# #%%
#
# # In addition, can also use the Overpass Turbo to get attic data but there is a limit in available memory. Speed is reasonable if query primary
# # highways in a city, but memory runs out if query all levels of highways.
# # Using the osmpy package to wrap around Overpass Turbo
# import osmpy
# boundary = gdf.geometry[0]
# query = """
#         [out:json][timeout:100][date:"2020-01-01T00:00:00Z"];
#         (
#           way["highway"="primary"](poly:"{boundary}");
#         );
#         make stats length=sum(length());
#         out;
#     """
# osmpy.get(query, boundary)
#
# # This is the query to use directly in the Overpass Turbo webpage
# query = """
#     [out:json][timeout:100][date:"2015-01-01T00:00:00Z"];
#     // fetch area “yiyang” to search in
#     {{geocodeArea:yiyang, hunan, china}}->.searchArea;
#     // gather results
#     (
#     way["highway"="primary"](area.searchArea);
#     );
#     make stats length=sum(length());
#     // print results
#     out geom;
#     """



