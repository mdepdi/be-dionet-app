import geopandas as gpd
import pandas as pd
import srtm
import gpxpy
import gpxpy.gpx
import math
import asyncio
from shapely.geometry import Point, Polygon, LineString, JOIN_STYLE, MultiPolygon
import geojson
import numpy as np
import json

from modules.market_share import marketShareType
from modules.market_share import marketShareType
from sqlalchemy.orm import sessionmaker
from database import *
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor

Session = sessionmaker(bind=engine)

def drawBuffer(lon, lat, radius):
    siteLoc = Point([lon, lat])
    siteLocGdf = gpd.GeoDataFrame(
        geometry=[siteLoc], crs='epsg:4326')
    siteLocBuffer = siteLocGdf.to_crs('epsg:3857')
    siteLocBuffer['geometry'] = siteLocBuffer['geometry'].buffer(radius)
    siteLocBuffer['area'] = siteLocBuffer['geometry'].area
    siteLocBuffer = siteLocBuffer.to_crs('epsg:4326')
    return siteLocBuffer


# def CoverageRadius(height):
#     if height <= 25:
#         radius = height * 19.2
#     elif height > 25 and height <= 32:
#         radius = ((height-25) * 45.7143)+480
#     elif height > 32 and height <= 42:
#         radius = ((height-32) * 64)+800
#     elif height > 42 and height <= 52:
#         radius = ((height-42) * 56)+1440
#     elif height > 52 and height <= 62:
#         radius = ((height-52) * 25)+2000
#     elif height > 62 and height <= 72:
#         radius = ((height-62) * 25)+2250
#     elif height > 72:
#         radius = 2500

#     return radius
def CoverageRadius(tower_height, h_min=5, h_max=72, r_min=0, r_max=2500):
    # Calculate the slope (m)
    m = (r_max - r_min) / (h_max - h_min)
    # Calculate the intercept (c)
    c = r_min - m * h_min
    # Calculate the coverage radius
    coverage_radius = m * tower_height + c

    # Ensure the radius doesn't exceed r_max
    return min(coverage_radius, r_max)


def sudut_antara_a_dan_c(tinggi_a, sisi_b):
    # Menghitung panjang sisi c menggunakan teorema Pythagoras
    sisi_c = math.sqrt(tinggi_a**2 + sisi_b**2)

    # Menghitung sudut antara sisi a dan sisi c dalam radian
    sudut_radian = math.atan2(tinggi_a, sisi_b)

    # Mengonversi sudut dari radian ke derajat
    sudut_derajat = math.degrees(sudut_radian)

    # Memastikan nilai sudut berada dalam rentang 0 hingga 360 derajat
    sudut_derajat %= 360

    return sudut_derajat


def panjang_sisi_b(sudut_derajat, a):
    # Mengonversi sudut dari derajat ke radian
    sudut_radian = math.radians(sudut_derajat)

    # Menghitung nilai tangen dari sudut
    tangen_sudut = math.tan(sudut_radian)

    # Menghitung panjang sisi "b"
    panjang_b = a / tangen_sudut
    return panjang_b


def polar_point(origin_point, angle, distance):
    return Point(origin_point.x + math.sin(math.radians(angle)) * distance,
                 origin_point.y + math.cos(math.radians(angle)) * distance)


def slice_line(line, segment_length):
    gdf_line = gpd.GeoDataFrame(geometry=[line], crs="epsg:4326")
    line_gdf = gdf_line.to_crs('epsg:3857')

    segments = []
    for i, row in line_gdf.iterrows():
        line = row['geometry']
        distance = 0

        while distance < line.length:
            point = line.interpolate(distance)
            segments.append(point)
            distance += segment_length

    segment_point = gpd.GeoDataFrame(geometry=segments, crs="epsg:3857")
    segment_point = segment_point.to_crs('epsg:4326')
    return segment_point

async def elevationProfile(elevation_data, Longitude, Latitude, line_sectoral):
    loop = asyncio.get_event_loop()
    executor = ThreadPoolExecutor()

    gdf = line_sectoral
    gpx = gpxpy.gpx.GPX()
    center = Point(Longitude, Latitude)
    center_gdf = gpd.GeoDataFrame(geometry=[center], crs="epsg:4326")
    center_gdf = await loop.run_in_executor(executor, center_gdf.to_crs, 'epsg:3857')

    # Create a track in the GPX file
    for index, row in gdf.iterrows():
        line = row['geometry']
        segmentedLine = await loop.run_in_executor(executor, slice_line, line, 50)

        track = gpxpy.gpx.GPXTrack()
        gpx.tracks.append(track)

        segment = gpxpy.gpx.GPXTrackSegment()
        track.segments.append(segment)

        for index, coords in segmentedLine.iterrows():
            point = gpxpy.gpx.GPXTrackPoint(
                latitude=coords.values[0].y,
                longitude=coords.values[0].x,
            )
            segment.points.append(point)

    gpx = gpxpy.parse(gpx.to_xml())
    await loop.run_in_executor(executor, elevation_data.add_elevations, gpx, True)

    elevation_profile = []
    for i, track in enumerate(gpx.tracks):
        for segment in track.segments:
            for point in segment.points:
                elevation_profile.append(
                    {'id': i, 'lon': point.longitude, 'lat': point.latitude, 'h': point.elevation})

    elevation_df = pd.DataFrame(elevation_profile)
    elevation_gdf = gpd.GeoDataFrame(elevation_profile, geometry=gpd.points_from_xy(
        elevation_df.lon, elevation_df.lat), crs='epsg:4326')
    elevation_gdf = await loop.run_in_executor(executor, elevation_gdf.to_crs, 'epsg:3857')
    elevation_gdf['distance_to_center'] = elevation_gdf['geometry'].distance(
        center_gdf['geometry'].values[0])
    elevation_gdf = await loop.run_in_executor(executor, elevation_gdf.to_crs, 'epsg:4326')
    return elevation_gdf


# def elevationProfile(elevation_data, Longitude, Latitude, line_sectoral):
#     gdf = line_sectoral
#     gpx = gpxpy.gpx.GPX()
#     center = Point(Longitude, Latitude)
#     center_gdf = gpd.GeoDataFrame(geometry=[center], crs="epsg:4326")
#     center_gdf = center_gdf.to_crs('epsg:3857')

#     # Create a track in the GPX file
#     for index, row in gdf.iterrows():
#         line = row['geometry']
#         segmentedLine = slice_line(line, 50)

#         track = gpxpy.gpx.GPXTrack()
#         gpx.tracks.append(track)

#         segment = gpxpy.gpx.GPXTrackSegment()
#         track.segments.append(segment)

#         for index, coords in segmentedLine.iterrows():
#             point = gpxpy.gpx.GPXTrackPoint(
#                 latitude=coords.values[0].y,
#                 longitude=coords.values[0].x,
#             )
#             segment.points.append(point)

#     gpx = gpxpy.parse(gpx.to_xml())
#     elevation_data.add_elevations(gpx, smooth=True)

#     elevation_profile = []
#     for i, track in enumerate(gpx.tracks):
#         for segment in track.segments:
#             for point in segment.points:
#                 elevation_profile.append(
#                     {'id': i, 'lon': point.longitude, 'lat': point.latitude, 'h': point.elevation})

#     elevation_df = pd.DataFrame(elevation_profile)
#     elevation_gdf = gpd.GeoDataFrame(elevation_profile, geometry=gpd.points_from_xy(
#         elevation_df.lon, elevation_df.lat), crs='epsg:4326')
#     elevation_gdf = elevation_gdf.to_crs('epsg:3857')
#     elevation_gdf['distance_to_center'] = elevation_gdf['geometry'].distance(
#         center_gdf['geometry'].values[0])
#     elevation_gdf = elevation_gdf.to_crs('epsg:4326')
#     return elevation_gdf


def genLineSectoral(Longitude, Latitude, steps, sectors, coverageRadius):
    start = 0  # start of circle in degrees
    end = 360  # end of circle in degrees
    center = Point(Longitude, Latitude)

    # prepare parameters
    if start > end:
        start = start - 360
    else:
        pass

    step_angle_width = (end - start) / steps
    sector_width = (end - start) / sectors
    steps_per_sector = int(math.ceil(steps / sectors))

    line_features = []

    for x in range(int(sectors)):
        radius = coverageRadius * 9.00737E-06
        segment_vertices = []
        # first the center and first point
        segment_vertices.append(polar_point(center, 0, 0))
        segment_vertices.append(polar_point(
            center, start + x * sector_width, radius))

        # add line segment from center to midpoint of each step
        for z in range(1, steps_per_sector):
            start_angle = start + x * sector_width + (z - 1) * step_angle_width
            end_angle = start + x * sector_width + z * step_angle_width
            midpoint_angle = (start_angle + end_angle) / 2
            segment_vertices.append(polar_point(
                center, midpoint_angle, radius))

        # then again the center point to finish the polygon
        segment_vertices.append(polar_point(
            center, start + x * sector_width + sector_width, radius))
        segment_vertices.append(polar_point(center, 0, 0))

        # create center line feature
        center_line_vertices = [polar_point(center, start + x * sector_width + sector_width / 2, 0), polar_point(
            center, start + x * sector_width + sector_width / 2, radius)]
        line_features.append(geojson.Feature(
            geometry=LineString(center_line_vertices)))

    # create GeoDataFrame for lines
    line_df = pd.DataFrame(line_features)
    line_gdf = gpd.GeoDataFrame(geometry=line_df['geometry'], crs='epsg:4326')
    return line_gdf


def topoDetention(towerHeight, radius, distance, reduceFactor):

    if reduceFactor != 0:
        sudut_ac = sudut_antara_a_dan_c(towerHeight, radius)
        radius = panjang_sisi_b(sudut_ac*reduceFactor, towerHeight)
        ratio = towerHeight/radius
        yRatio = distance*ratio
        yHeight = towerHeight-yRatio
    else:
        yHeight = towerHeight

    return yHeight
async def process_topo_detention(distance, towerHeight, coverageRadius, reduceFactor, centerHeight):
    topoCheck = topoDetention(towerHeight, coverageRadius, distance, reduceFactor)
    return topoCheck + centerHeight

async def process_sector(i, elevationSectoral, topoDetentions, coverageRadius):
    sectoralElevation = elevationSectoral[elevationSectoral['id'] == i].reset_index()
    d = coverageRadius
    td = []
    elSegment = []


    for id, row in sectoralElevation.iterrows():
        if row['h'] is not None:
            if topoDetentions[id] > row['h']:
                if row['distance_to_center'] > coverageRadius:
                    break
                else:
                    elSegment.append(row['h'])
                    d = row['distance_to_center']
                    td.append(topoDetentions[id])
            elif topoDetentions[id] < row['h']:
                break

    for id, row in sectoralElevation.iterrows():
        sectoralElevation.at[id,'tdh'] = topoDetentions[id]
        sectoralElevation.at[id,'max_distance'] = round(d,1)

    return d, td, min(elSegment), max(elSegment), sectoralElevation



async def process_sector_area(x, center, start, sector_width, step_angle_width, steps_per_sector, maxDistance):
    rad = maxDistance[x] * 9.00737E-06
    segment_vertices = []

    # first the center and first point
    segment_vertices.append(polar_point(center, 0, 0))
    segment_vertices.append(polar_point(center, start + x * sector_width, rad))

    # add line segment from center to midpoint of each step
    for z in range(1, steps_per_sector):
        start_angle = start + x * sector_width + (z - 1) * step_angle_width
        end_angle = start + x * sector_width + z * step_angle_width
        midpoint_angle = (start_angle + end_angle) / 2
        segment_vertices.append(polar_point(center, midpoint_angle, rad))

    # then again the center point to finish the polygon
    segment_vertices.append(polar_point(center, start + x * sector_width + sector_width, rad))
    segment_vertices.append(polar_point(center, 0, 0))

    # create polygon feature
    polygon_feature = geojson.Feature(geometry=Polygon(segment_vertices))

    # create center line feature
    center_line_vertices = [
        polar_point(center, start + x * sector_width + sector_width / 2, 0),
        polar_point(center, start + x * sector_width + sector_width / 2, rad)
    ]
    lineString = LineString(center_line_vertices)
    lastCoordinates = lineString.coords[-1]

    return polygon_feature, lastCoordinates

async def sectoralElevationProfile(Longitude, Latitude, towerHeight, steps, sectors, reduceFactor, coverageRadius, bufferArea):
    loop = asyncio.get_event_loop()
    executor = ThreadPoolExecutor()

    elevation_data = srtm.get_data()

    centerHeight = elevation_data.get_elevation(Latitude, Longitude)
    lineSectoral = genLineSectoral(
        Longitude, Latitude, steps, sectors, coverageRadius)
    elevationSectoral = await elevationProfile(
        elevation_data, Longitude, Latitude, lineSectoral)

    elevationSectoral = elevationSectoral[elevationSectoral['distance_to_center'] <= coverageRadius].reset_index(drop=True)
    distanceList = list(elevationSectoral['distance_to_center'].unique())

    topoDetentions = []

    async with asyncio.TaskGroup() as tg:
        tasks = [tg.create_task(process_topo_detention(distance, towerHeight, coverageRadius, reduceFactor, centerHeight)) for distance in distanceList]

        for task in tasks:
            topoDetentions.append(await task)

    lineId = list(elevationSectoral['id'].unique())

    topoDetentionHeight, maxDistance, minElevation, maxElevation = [],[], [], []
    allElevations = gpd.GeoDataFrame()

    async with asyncio.TaskGroup() as tg:
        tasks = [tg.create_task(process_sector(i, elevationSectoral, topoDetentions, coverageRadius)) for i in lineId]

        for task in tasks:
            d, td, min_el, max_el, line_elevation= await task
            topoDetentionHeight.append(td)
            maxDistance.append(d)
            minElevation.append(min_el)
            maxElevation.append(max_el)
            allElevations = pd.concat([allElevations, line_elevation], ignore_index=True)

    allElevations['h'] = allElevations['h'].apply(lambda x: round(x, 1))
    allElevations['tdh'] = allElevations['tdh'].apply(lambda x: round(x, 1))
    allElevations['distance_to_center'] = allElevations['distance_to_center'].apply(lambda x: round(x, 1))
    # allElevations['max_distance'] = maxDistance
    # allElevations['max_distance'] = allElevations['max_distance'].apply(lambda x: round(x, 1))

    start = 0  # start of circle in degrees
    end = 360  # end of circle in degrees
    center = Point(Longitude, Latitude)

    # prepare parameters
    if start > end:
        start = start - 360
    else:
        pass

    step_angle_width = (end - start) / steps
    sector_width = (end - start) / sectors
    steps_per_sector = int(math.ceil(steps / sectors))

    polygon_features = []
    last_coordinates = []

    async with asyncio.TaskGroup() as tg:
        tasks = [
            tg.create_task(process_sector_area(x, center, start, sector_width, step_angle_width, steps_per_sector, maxDistance))
            for x in range(int(sectors))
        ]

        for task in tasks:
            polygon_feature, lastCoordinates = await task
            polygon_features.append(polygon_feature)
            last_coordinates.append(lastCoordinates)

    polygon_df = pd.DataFrame(polygon_features)
    polygon_gdf = gpd.GeoDataFrame(
        geometry=polygon_df['geometry'], crs='epsg:4326')

    # # Dissolve the polygons into a single polygon
    dissolved_polygon = polygon_gdf.unary_union
    dissolved_gdf = gpd.GeoDataFrame(
        geometry=[dissolved_polygon], crs='epsg:4326')
    dissolved_gdf['idis'] = 1
    dissolved_gdf = dissolved_gdf.to_crs('epsg:3857')
    buffer_distance = 1  # Adjust this value as needed; it controls the size of the buffer
    dissolved_gdf['geometry'] = dissolved_gdf['geometry'].buffer(
        buffer_distance)
    dissolved_gdf = dissolved_gdf.dissolve(by='idis')
    dissolved_gdf['geometry'] = dissolved_gdf['geometry'].buffer(
        -buffer_distance)
    dissolved_gdf['area'] = dissolved_gdf['geometry'].area
    dissolved_gdf = dissolved_gdf.to_crs('epsg:4326')

    points = []
    for x, y in last_coordinates:
        point = Point(x, y)
        points.append(point)

    # Now, 'points' list contains Shapely Point objects for each coordinate
    lastCoords = gpd.GeoDataFrame(geometry=points, crs='epsg:4326')
    lastCoords['id'] = range(len(lastCoords))
    lastCoords['distance'] = maxDistance
    lastCoords['minElevation'] = minElevation
    lastCoords['maxElevation'] = maxElevation

    CoveragePerBufferArea = round(
        (round(dissolved_gdf['area'].sum()) / round(bufferArea['area'].values[0])), 2)
    if CoveragePerBufferArea >= 1:
        CoveragePerBufferArea = 1

    elevationRecap = {'coverageArea': dissolved_gdf['area'].sum(), 'topTowerElevation': centerHeight+towerHeight, 'minCoverageElevation': round(min(
        lastCoords['minElevation'])), 'maxCoverageElevation': round(max(lastCoords['maxElevation'])), 'coverageRadius': coverageRadius, 'coveragePerBufferArea': CoveragePerBufferArea}

    dissolved_gdf.reset_index(drop=True)

    # return dissolved_gdf, lastCoords, lineSectoral, elevationSectoral, elevationRecap
    return dissolved_gdf,lineSectoral, allElevations, elevationRecap



def genSectoral(Longitude, Latitude, steps, sectors, radius):
    start = 0  # start of circle in degrees
    end = 360  # end of circle in degrees
    center = Point(Longitude, Latitude)

    # prepare parameters
    if start > end:
        start = start - 360
    else:
        pass

    step_angle_width = (end - start) / steps
    sector_width = (end - start) / sectors
    steps_per_sector = int(math.ceil(steps / sectors))

    polygon_features = []
    sector = []
    last_coordinates = []

    for x in range(int(sectors)):
        sector.append(x+1)

        rad = radius * 9.00737E-06
        segment_vertices = []

        # first the center and first point
        segment_vertices.append(polar_point(center, 0, 0))
        segment_vertices.append(polar_point(
            center, start + x * sector_width, rad))

        # add line segment from center to midpoint of each step
        for z in range(1, steps_per_sector):
            start_angle = start + x * sector_width + (z - 1) * step_angle_width
            end_angle = start + x * sector_width + z * step_angle_width
            midpoint_angle = (start_angle + end_angle) / 2
            segment_vertices.append(polar_point(center, midpoint_angle, rad))

        # then again the center point to finish the polygon
        segment_vertices.append(polar_point(
            center, start + x * sector_width + sector_width, rad))
        segment_vertices.append(polar_point(center, 0, 0))

        # create polygon feature
        polygon_features.append(geojson.Feature(
            geometry=Polygon(segment_vertices)))

        # create center line feature
        center_line_vertices = [polar_point(center, start + x * sector_width + sector_width / 2, 0),
                                polar_point(center, start + x * sector_width + sector_width / 2, rad)]
        lineString = LineString(center_line_vertices)
        lastCoordinates = lineString.coords[-1]
        last_coordinates.append(lastCoordinates)

    polygon_df = pd.DataFrame(polygon_features)
    polygon_gdf = gpd.GeoDataFrame(
        geometry=polygon_df['geometry'], crs='epsg:4326')
    polygon_gdf['kuadran'] = sector
    polygon_gdf['center_vertices'] = last_coordinates

    return polygon_gdf


def genQuadranSectoral(site_loc, data):

    data = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data.longitude, data.latitude), crs='epsg:4326')
    steps = 30
    sectors = 12.0
    start = 0.0
    end = 360.0

    # prepare parameters
    if start > end:
        start = start - 360

    step_angle_width = (end - start) / steps
    sector_width = (end - start) / sectors
    steps_per_sector = int(math.ceil(steps / sectors))

    # Determine quadrant surrounding
    kuadran = []
    azimuth = []
    for i, row in data.iterrows():
        # Calculate Azimuth
        az = np.degrees(np.arctan2(
            (row['longitude'] - site_loc.x), (row['latitude'] - site_loc.y)))
        az = az if az >= 0 else az + 360
        azimuth.append(az)

        k = 0

        # Calculate Kuadran
        if 0 <= az < 30:
            k = 1
        elif 30 <= az < 60:
            k = 2
        elif 60 <= az < 90:
            k = 3
        elif 90 <= az < 120:
            k = 4
        elif 120 <= az < 150:
            k = 5
        elif 150 <= az < 180:
            k = 6
        elif 180 <= az < 210:
            k = 7
        elif 210 <= az < 240:
            k = 8
        elif 240 <= az < 270:
            k = 9
        elif 270 <= az < 300:
            k = 10
        elif 300 <= az < 330:
            k = 11
        elif 330 <= az <= 360:
            k = 12

        kuadran.append(k)

    # Ensure using .loc for setting values
    data = data.copy()
    data.loc[:, 'kuadran'] = kuadran
    data.loc[:, 'kuadran+1'] = [x + 1 for x in kuadran]
    data.loc[:, 'kuadran-1'] = [x - 1 for x in kuadran]
    data.loc[data['kuadran+1'] == 13, 'kuadran+1'] = 1
    data.loc[data['kuadran-1'] == 0, 'kuadran-1'] = 12
    data.loc[:, 'kuadran+2'] = [x + 2 for x in kuadran]
    data.loc[:, 'kuadran-2'] = [x - 2 for x in kuadran]
    data.loc[data['kuadran+2'] == 14, 'kuadran+2'] = 2
    data.loc[data['kuadran-2'] == -1, 'kuadran-2'] = 11

    # data = data.drop('Unnamed: 0', axis=1)
    final_df = []
    features = []

    for index, row in data.iterrows():
        siteid = row['site_id']
        kuadran = row['kuadran']
        for x in range(0, int(sectors)):
            center = site_loc
            radius = row['buffer'] * 9.00737E-06

            segment_vertices = []

            # first the center and first point
            segment_vertices.append(polar_point(center, 0, 0))
            segment_vertices.append(polar_point(
                center, start + x*sector_width, radius))

            # then the sector outline points
            for z in range(1, steps_per_sector):
                segment_vertices.append(
                    (polar_point(center, start + x * sector_width + z * step_angle_width, radius)))

            # then again the center point to finish the polygon
            segment_vertices.append(polar_point(
                center, start + x * sector_width+sector_width, radius))
            segment_vertices.append(polar_point(center, 0, 0))

            geometry = Polygon(segment_vertices)

            # create feature
            features.append({'geometry': geometry, 'site_id': str(siteid), 'kuadran': int(x),
                            'iterasi': index, 'distance':row['distance'], 'buffer': row['buffer']})

    features = gpd.GeoDataFrame(features)
    final_df.append(features)

    final_df = pd.concat(final_df)
    features = final_df
    # features['site_id'] = features['site_id']
    features['kuadran'] = features['kuadran'] + 1
    features = features.reset_index(drop=True)
    gdf = gpd.GeoDataFrame(features, geometry=(features["geometry"]))

    gdf_result = []

    for i, row in data.iterrows():

        s1 = gdf[(gdf['iterasi'] == i) & (gdf['kuadran'] == row['kuadran'])]
        s1['type'] = 1
        gdf_result.append(s1)
        s2 = gdf[(gdf['iterasi'] == i) & (gdf['kuadran'] == row['kuadran+1'])]
        s2['type'] = 2
        gdf_result.append(s2)
        s3 = gdf[(gdf['iterasi'] == i) & (gdf['kuadran'] == row['kuadran-1'])]
        s3['type'] = 2
        gdf_result.append(s3)
        s4 = gdf[(gdf['iterasi'] == i) & (gdf['kuadran'] == row['kuadran+2'])]
        s4['type'] = 3
        gdf_result.append(s4)
        s5 = gdf[(gdf['iterasi'] == i) & (gdf['kuadran'] == row['kuadran-2'])]
        s5['type'] = 3
        gdf_result.append(s5)

    sCon = pd.concat(gdf_result, ignore_index=True)
    gdf_final = gpd.GeoDataFrame(sCon)
    gdf_final = gdf_final.set_crs('epsg:4326')
    gdf_final = gdf_final.sort_values(by=['type','buffer','kuadran'], ascending=[True,True, True])

    final = gpd.GeoDataFrame()
    for kd in range(1,13):
        kdf = gdf_final[gdf_final['kuadran'] == kd].reset_index(drop=True)
        final = pd.concat([final, kdf.head(1)], ignore_index=True)

    final = final.to_crs('epsg:3857')
    final['area'] = final['geometry'].area
    final = final.to_crs('epsg:4326')

    startKuadran = []

    for kd in range(1,13):
        obj = {"kuadran": kd}
        if kd == 10:
            start = final[final['kuadran'].isin([10,11,12,1])].reset_index(drop=True)
            obj['sum_distance'] = start['distance'].sum()
        elif kd == 11:
            start = final[final['kuadran'].isin([11,12,1,2])].reset_index(drop=True)
            obj['sum_distance'] = start['distance'].sum()
        elif kd == 12:
            start = final[final['kuadran'].isin([12,1,2,3])].reset_index(drop=True)
            obj['sum_distance'] = start['distance'].sum()
        else:
            start = final[final['kuadran'].isin(range(kd, kd+4))].reset_index(drop=True)
            obj['sum_distance'] = start['distance'].sum()

        startKuadran.append(obj)

    startKuadran = pd.DataFrame(startKuadran)
    startKuadran = startKuadran.sort_values(by=['sum_distance'], ascending=False).reset_index(drop=True)
    startKuadran = startKuadran['kuadran'].values[0]

    def get_sector_ranges(start):
        sector_ranges = []
        for i in range(3):
            sector_ranges.append([((start + j) % 12) + 1 for j in range(4)])
            start = (start + 4) % 12
        return sector_ranges


    sector_ranges = get_sector_ranges(startKuadran-1)
    sector1 = final[final['kuadran'].isin(sector_ranges[0])].reset_index(drop=True)
    sector1['sector'] = 1
    sector2 = final[final['kuadran'].isin(sector_ranges[1])].reset_index(drop=True)
    sector2['sector'] = 2
    sector3 = final[final['kuadran'].isin(sector_ranges[2])].reset_index(drop=True)
    sector3['sector'] = 3

    secotral = pd.concat([sector1, sector2, sector3], ignore_index=True)
    secotral = secotral.sort_values(by=['sector','type','distance'], ascending=[True, True, True]).reset_index(drop=True)
    secotral = secotral.drop_duplicates(subset=['sector'], keep='first')
    # data = data.drop('Unnamed: 0', axis=1)
    sectoral_df = []
    sectoral_features = []

    sectoral_sectors = 3
    sectoral_steps = 120

    if startKuadran == 1:
        sectoral_start = 0.0
        sectoral_end = 360.0
    elif startKuadran == 2:
        sectoral_start = 30.0
        sectoral_end = sectoral_start+360
    elif startKuadran == 3:
        sectoral_start = 60.0
        sectoral_end = sectoral_start+360
    elif startKuadran == 4:
        sectoral_start = 90.0
        sectoral_end = sectoral_start+360
    elif startKuadran == 5:
        sectoral_start = 120.0
        sectoral_end = sectoral_start+360
    elif startKuadran == 6:
        sectoral_start = 150.0
        sectoral_end = sectoral_start+360
    elif startKuadran == 7:
        sectoral_start = 180.0
        sectoral_end = sectoral_start+360
    elif startKuadran == 8:
        sectoral_start = 210.0
        sectoral_end = sectoral_start+360
    elif startKuadran == 9:
        sectoral_start = 240.0
        sectoral_end = sectoral_start+360
    elif startKuadran == 10:
        sectoral_start = 270.0
        sectoral_end = sectoral_start+360
    elif startKuadran == 11:
        sectoral_start = 300.0
        sectoral_end = sectoral_start+360
    elif startKuadran == 12:
        sectoral_start = 330.0
        sectoral_end = sectoral_start+360

    if sectoral_start > sectoral_end:
        sectoral_start = sectoral_start - 360
    # Ensure the end angle wraps around if it exceeds 360 degrees

    step_angle_width = (sectoral_end - sectoral_start) / sectoral_steps
    sector_width = (sectoral_end - sectoral_start) / sectoral_sectors
    steps_per_sector = int(math.ceil(sectoral_steps / sectoral_sectors))

    for index, row in secotral.iterrows():
        kuadran = row['sector']
        for x in range(0, int(sectoral_sectors)):
            center = site_loc
            radius = row['buffer'] * 9.00737E-06
            segment_vertices = []
            # first the center and first point
            segment_vertices.append(polar_point(center, 0, 0))
            segment_vertices.append(polar_point(
                center, sectoral_start + x*sector_width, radius))

            # then the sector outline points
            for z in range(1, steps_per_sector):
                segment_vertices.append(
                    (polar_point(center, sectoral_start + x * sector_width + z * step_angle_width, radius)))

            # then again the center point to finish the polygon
            segment_vertices.append(polar_point(
                center, sectoral_start + x * sector_width+sector_width, radius))
            segment_vertices.append(polar_point(center, 0, 0))

            geometry = Polygon(segment_vertices)

            # create feature
            sectoral_features.append({'geometry': geometry, 'kuadran': int(x), 'sector':row["sector"],'distance':row['distance'], 'buffer': row['buffer']})

    sectoral_features = gpd.GeoDataFrame(sectoral_features)
    sectoral_df.append(sectoral_features)

    sectoral_df = pd.concat(sectoral_df)
    sectoral_df_filtered = sectoral_df.copy()
    sectoral_df_filtered = sectoral_df_filtered[
    ((sectoral_df_filtered['kuadran'] == 0) & (sectoral_df_filtered['sector'] == 1)) |
    ((sectoral_df_filtered['kuadran'] == 1) & (sectoral_df_filtered['sector'] == 2)) |
    ((sectoral_df_filtered['kuadran'] == 2) & (sectoral_df_filtered['sector'] == 3))
    ].reset_index(drop=True)
    sectoral_df_filtered = sectoral_df_filtered.set_crs('epsg:4326')
    sectoral_df_filtered['kuadran'] = sectoral_df_filtered['kuadran'] + 1

    for i, row in sectoral_df_filtered.iterrows():
        if row['kuadran'] == 1:
            azimuth_start = sectoral_start
            if azimuth_start > 360:
                azimuth_start = azimuth_start-360

            azimuth_end = azimuth_start+120
            if azimuth_end > 360:
                azimuth_end = azimuth_end-360

            sectoral_df_filtered.at[i, 'azimuth_start'] = azimuth_start
            sectoral_df_filtered.at[i, 'azimuth_end'] = azimuth_end
        elif row['kuadran'] == 2:
            azimuth_start = sectoral_start+120
            if azimuth_start > 360:
                azimuth_start = azimuth_start-360
            azimuth_end = azimuth_start+120
            if azimuth_end > 360:
                azimuth_end = azimuth_end-360

            sectoral_df_filtered.at[i, 'azimuth_start'] = azimuth_start
            sectoral_df_filtered.at[i, 'azimuth_end'] = azimuth_end
        elif row['kuadran'] == 3:
            azimuth_start = sectoral_start+240
            if azimuth_start > 360:
                azimuth_start = azimuth_start-360
            azimuth_end = azimuth_start+120
            if azimuth_end > 360:
                azimuth_end = azimuth_end-360
            sectoral_df_filtered.at[i, 'azimuth_start'] = azimuth_start
            sectoral_df_filtered.at[i, 'azimuth_end'] = azimuth_end

    return sectoral_df_filtered

def dRatio(ka, kb, quad_distance):
    a = quad_distance[quad_distance['kuadran'] == ka]["distance"].values[0]
    b = quad_distance[quad_distance['kuadran'] == kb]["distance"].values[0]
    mind = min([a,b])
    maxd = max([a,b])
    ratio = (mind/maxd)*100
    return ratio

def strObjToJson(strObj):
    return json.loads(strObj.replace("'",'"'))


def genMarketShareCompetition(site_loc, operator, nearest_surrounding_site, mColopriming, mOperatorData, village):
    comid_village = village['comid_village'].values[0]

    nearest_surrounding_distance = nearest_surrounding_site['distance'].values[0]

    if nearest_surrounding_distance < 1000:
        siteLocBufferCompetitor = drawBuffer(
            site_loc.x, site_loc.y, 500)
    if nearest_surrounding_distance > 1000:
        siteLocBufferCompetitor = drawBuffer(
            site_loc.x, site_loc.y, 1000)

    operatorWithin = gpd.overlay(
        mOperatorData, siteLocBufferCompetitor, how='intersection').reset_index(drop=True)
    operatorWithinList = sorted(set(list(operatorWithin['operator'])))

    operatorCountList = operatorWithinList

    if len(operatorCountList) == 0 or operator not in operatorCountList:
        operatorCountList.append(operator)

    operatorCountList = sorted(operatorCountList)

    coloprimingWithin = gpd.overlay(mColopriming, siteLocBufferCompetitor, how='intersection').reset_index(drop=True)
    coloprimingWithin["tenant"] = coloprimingWithin["tenant"].apply(strObjToJson)

    tenantList = []

    for i , row in coloprimingWithin.iterrows():
        tenantList += row['tenant']['tenant']

    operatorCountList = list(set(operatorCountList + tenantList))
    ms_type = marketShareType(operatorCountList)
    with Session() as session:
        query = session.query(getattr(mMarketShare, ms_type)).filter_by(comid_village=comid_village)
        results = query.all()
        ms = pd.DataFrame(results, columns=[ms_type])
        ms = ms[ms_type].apply(strObjToJson)[0]
    return ms

def genArpuAf(site_loc, operator):
    with Session() as session:
        query = text(f"""
            SELECT *,
                ST_Distance(geometry, ST_GeomFromText('{site_loc}', 4326)) AS distance
            FROM m_arpu_af
            WHERE operator = '{operator}'
            ORDER BY geometry <-> ST_GeomFromText('{site_loc}', 4326)
            LIMIT 1;
        """)

        village_operator = session.execute(query)
        village_operator = village_operator.fetchall()
        village_operator = pd.DataFrame(village_operator)
        arpu = village_operator['arpu'].values[0]
        area_factor = village_operator['area_factor'].values[0]

    return [arpu, area_factor]


def genReverseRevenue(total_sectoral_population, sectoral_population, market_share_competition, arpu, area_factor, operator):
    market_share_opeator = market_share_competition[operator]
    total_revenue = total_sectoral_population*market_share_opeator*arpu*area_factor

    for i, row in sectoral_population.iterrows():
        sectoral_population.at[i, 'total_revenue'] = int(row['total_population'] * market_share_opeator * arpu * area_factor)

    grade = 'BRONZE'

    if operator == 'TSEL':
        if total_revenue > 0:
            if total_revenue <= 60000000:
                grade = 'BRONZE'
            elif total_revenue > 60000000 and total_revenue <= 100000000:
                grade = 'SILVER'
            elif total_revenue > 100000000 and total_revenue <= 200000000:
                grade = 'GOLD'
            elif total_revenue > 200000000:
                grade = 'PLATINUM'

    elif operator == 'XL' or operator == 'IOH':
        if total_revenue > 0:
            if total_revenue <= 50000000:
                grade = 'BRONZE'
            elif total_revenue > 50000000 and total_revenue <= 75000000:
                grade = 'SILVER'
            elif total_revenue > 75000000 and total_revenue <= 100000000:
                grade = 'GOLD'
            elif total_revenue > 100000000:
                grade = 'PLATINUM'

    elif operator == 'SF':
        if total_revenue > 0:
            if total_revenue <= 25000000:
                grade = 'BRONZE'
            elif total_revenue > 25000000 and total_revenue <= 50000000:
                grade = 'SILVER'
            elif total_revenue > 50000000 and total_revenue <= 750000000:
                grade = 'GOLD'
            elif total_revenue > 750000000:
                grade = 'PLATINUM'

    return [total_revenue, sectoral_population, grade]


def genRevenue(total_sectoral_population, sectoral_population, market_share_competition, arpu, area_factor, operator, surrounding_sectoral):
    if 'actual_revenue' in surrounding_sectoral.columns:
        surrounding_sectoral['actual_revenue'] = surrounding_sectoral['actual_revenue'].astype(float)
        surrounding_sectoral = surrounding_sectoral[surrounding_sectoral['actual_revenue'] > 0]

    market_share_opeator = market_share_competition[operator]

    total_revenue = total_sectoral_population * market_share_opeator * arpu * area_factor

    # pass the max actual revenue
    pass_max_actual_revenue = 0

    try:
        max_actual_revenue = max(surrounding_sectoral['actual_revenue'])

        if total_revenue > max_actual_revenue:
            total_revenue = max_actual_revenue*0.85
            pass_max_actual_revenue = 1
    except Exception as e:
        pass

    for i, row in sectoral_population.iterrows():
        sectoral_population.at[i, 'total_revenue'] = int(row['total_population'] * market_share_opeator * arpu * area_factor)

    grade = 'BRONZE'

    if operator == 'TSEL':
        if total_revenue > 0:
            if total_revenue <= 60000000:
                grade = 'BRONZE'
            elif total_revenue > 60000000 and total_revenue <= 100000000:
                grade = 'SILVER'
            elif total_revenue > 100000000 and total_revenue <= 200000000:
                grade = 'GOLD'
            elif total_revenue > 200000000:
                grade = 'PLATINUM'

    elif operator == 'XL' or operator == 'IOH':
        if total_revenue > 0:
            if total_revenue <= 50000000:
                grade = 'BRONZE'
            elif total_revenue > 50000000 and total_revenue <= 75000000:
                grade = 'SILVER'
            elif total_revenue > 75000000 and total_revenue <= 100000000:
                grade = 'GOLD'
            elif total_revenue > 100000000:
                grade = 'PLATINUM'

    elif operator == 'SF':
        if total_revenue > 0:
            if total_revenue <= 25000000:
                grade = 'BRONZE'
            elif total_revenue > 25000000 and total_revenue <= 50000000:
                grade = 'SILVER'
            elif total_revenue > 50000000 and total_revenue <= 750000000:
                grade = 'GOLD'
            elif total_revenue > 750000000:
                grade = 'PLATINUM'

    return [total_revenue, sectoral_population, grade,pass_max_actual_revenue]


