import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

import geopandas as gpd
from sqlalchemy import text
from database import engine
import json
from shapely import Point, wkt, wkb, LineString
import numpy as np

from modules.colopriming_modules import *
from fastapi import HTTPException

from pydantic import ValidationError
from modules.market_share import marketShareType
from database import *

import osmnx as ox
import networkx as nx
import momepy
import ast

from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import KMeans

import asyncio
from concurrent.futures import ThreadPoolExecutor

import traceback
import gpxpy
import gpxpy.gpx
import xml.etree.ElementTree as ET
import srtm
import warnings

warnings.filterwarnings("ignore")

async def getExistingSite(site_loc):
    default_values = {
        "opt_tenant": [], 
        "tenant_revenue": {
            "TSEL": 0,
            "XL": 0,
            "IOH": 0,
            "SF": 0
        }, 
        "colo_antena_height": 0
    }
    try:
        radius = 10
        query = text(
            f"""
            SELECT 
                opt.site_id,
                opt.site_name,
                opt.tower_height,
                opt.longitude,
                opt.latitude,
                opt.actual_revenue,
                opt.operator,
                ST_Distance(
                    ST_Transform(ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326), 3857),
                    ST_Transform(opt.geometry, 3857)
                ) AS distance
            FROM m_operator AS opt
            WHERE
                ST_Intersects(
                    ST_Transform(opt.geometry, 3857),
                    ST_Buffer(
                        ST_Transform(
                            ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326),
                            3857
                        ),
                        {radius}
                    )
                )
            """
        )

        async with SessionLocal() as session:       
            nearest_opt = await session.execute(query)
            nearest_opt = nearest_opt.fetchall()
        
        nearest_opt_site = pd.DataFrame(nearest_opt)
        nearest_opt_site = gpd.GeoDataFrame(nearest_opt_site, geometry=gpd.points_from_xy(nearest_opt_site.longitude, nearest_opt_site.latitude), crs='EPSG:4326')
        nearest_opt_site['source'] = 2
        nearest_opt_site['antena_height'] = nearest_opt_site['tower_height']
        default_values['opt_tenant'] = list(nearest_opt_site['operator'].unique())
        default_values['colo_antena_height'] = nearest_opt_site['tower_height'][0]
        nearest_opt_site['colo_antena_height'] = nearest_opt_site['tower_height'][0]

        for i, row in nearest_opt_site.iterrows():
            default_values['tenant_revenue'][row['operator']] = row['actual_revenue']
        
        queryColo= text(
            f"""
            SELECT 
                colo.site_id,
                colo.site_name,
                colo.tower_height,
                colo.longitude,
                colo.latitude,
                colo.actual_revenue,
                colo.antena_height,
                colo.tenant,
                ST_Distance(
                    ST_Transform(ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326), 3857),
                    ST_Transform(colo.geometry, 3857)
                ) AS distance
            FROM m_colopriming AS colo
            WHERE
                ST_Intersects(
                    ST_Transform(colo.geometry, 3857),
                    ST_Buffer(
                        ST_Transform(
                            ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326),
                            3857
                        ),
                        {radius}
                    )
                )
            """
        )

        async with SessionLocal() as session:       
            nearest_colo = await session.execute(queryColo)
            nearest_colo = nearest_colo.fetchall()
        
        nearest_colo_site = pd.DataFrame(nearest_colo)
        nearest_colo_site = gpd.GeoDataFrame(nearest_colo_site, geometry=gpd.points_from_xy(nearest_colo_site.longitude, nearest_colo_site.latitude), crs='EPSG:4326')
        
        nearest_colo_opt = pd.DataFrame()
        
        for i, row in nearest_colo_site.iterrows():
            df_row = row.to_frame().T
            df_row = df_row.drop(['tenant','actual_revenue'], axis=1)
            arevene = strObjToJson(row['actual_revenue'])
            tenant = strObjToJson(row["tenant"])['tenant']
            for t in tenant:
                df_row['operator'] = t
                df_row['actual_revenue'] = arevene[t]
                df_row['source'] = 1
                nearest_colo_opt = pd.concat([nearest_colo_opt, df_row], ignore_index=True)
        

        nearest = pd.concat([nearest_opt_site, nearest_colo_opt], ignore_index=True)
        nearest = nearest.sort_values(by=['operator','distance','source'])
        nearest = nearest.drop_duplicates(subset=['operator'], keep='first')

        opt_tenant = []
        tenant_revenue = default_values["tenant_revenue"].copy()
        colo_antena_height = 0

        for i, row in nearest.iterrows():
            opt_tenant.append(row['operator'])
            tenant_revenue[row['operator']] = row['actual_revenue']
            colo_antena_height = row['antena_height']
        opt_tenant = default_values['opt_tenant'].copy()
        tenant_revenue = default_values["tenant_revenue"].copy()
        colo_antena_height = default_values["colo_antena_height"]

        
        return opt_tenant, tenant_revenue, colo_antena_height
    
    except Exception as e:
        print(f'error getExistingSite: {e}')
        # raise
        opt_tenant = default_values["opt_tenant"]
        tenant_revenue = default_values["tenant_revenue"]
        colo_antena_height = default_values["colo_antena_height"]
        return opt_tenant, tenant_revenue, colo_antena_height


async def genLos(site_loc, tower_height, operator, vegetation_height):
    try:
        gdf_site_loc = gpd.GeoDataFrame(geometry=[site_loc], crs='epsg:4326')
        radius = 10000
        sectoral = genSectoral(site_loc.x, site_loc.y,  90, 4, radius)
        query = text(
            f"""
            SELECT 
                opt.site_id,
                opt.tower_height,
                opt.longitude,
                opt.latitude,
                ST_Distance(
                    ST_Transform(ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326), 3857),
                    ST_Transform(opt.geometry, 3857)
                ) AS distance
            FROM m_operator AS opt
            WHERE
                opt.operator = '{operator}' AND
                ST_Intersects(
                    ST_Transform(opt.geometry, 3857),
                    ST_Buffer(
                        ST_Transform(
                            ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326),
                            3857
                        ),
                        {radius}
                    )
                )
            """
        )

        async with SessionLocal() as session:       
            nearest_opt = await session.execute(query)
            nearest_opt = nearest_opt.fetchall()
        
        nearest_opt_site = pd.DataFrame(nearest_opt)
        nearest_opt_site = gpd.GeoDataFrame(nearest_opt_site, geometry=gpd.points_from_xy(nearest_opt_site.longitude, nearest_opt_site.latitude), crs='EPSG:4326')
        nearest_opt_site = nearest_opt_site[nearest_opt_site['distance']>100].reset_index(drop=True)
        nearest_opt_site = gpd.overlay(nearest_opt_site, sectoral[['kuadran','geometry']], how='intersection')
        nearest_opt_site = nearest_opt_site.sort_values(by=['kuadran','distance'], ascending=True).reset_index(drop=True)
        nearest_opt_site['rank'] = nearest_opt_site.groupby('kuadran')['distance'].rank(method='first', ascending=True)
        nearest_opt_site = nearest_opt_site[nearest_opt_site['rank'] <= 3].reset_index(drop=True)
            
        gdf_site_loc = gdf_site_loc.to_crs('epsg:3857')
        
        lines = gpd.GeoDataFrame()
        gpx = gpxpy.gpx.GPX()
        
        for i, row in nearest_opt_site.iterrows():
            elevation_data = srtm.get_data()
            line = LineString([site_loc, row['geometry']])
            sliced_line = slice_line(line, 30)
            sliced_line.iloc[-1] = row['geometry']
            sliced_line['pid'] = range(1, len(sliced_line) + 1)
            sliced_line['kuadran'] = row['kuadran']
            sliced_line['rank'] = int(row['rank'])

            track = gpxpy.gpx.GPXTrack()
            gpx.tracks.append(track)

            segment = gpxpy.gpx.GPXTrackSegment()
            track.segments.append(segment)

            for index, coords in sliced_line.iterrows():
                point = gpxpy.gpx.GPXTrackPoint(
                    latitude=coords['geometry'].y,
                    longitude=coords['geometry'].x,
                )

                    # Create XML elements for custom attributes
                kuadran_elem = ET.Element('kuadran')
                kuadran_elem.text = str(coords['kuadran'])
                pid_elem = ET.Element('pid')
                pid_elem.text = str(coords['pid'])
                
                rank_elem = ET.Element('rank')
                rank_elem.text = str(coords['rank'])

                # Append XML elements to extensions
                point.extensions.append(kuadran_elem)
                point.extensions.append(pid_elem)
                point.extensions.append(rank_elem)

                segment.points.append(point)

        gpx = gpxpy.parse(gpx.to_xml())
        elevation_data.add_elevations(gpx, smooth=True)

        elevation_profile = []
        for i, track in enumerate(gpx.tracks):
            for segment in track.segments:
                for point in segment.points:
                    pid = None
                    kuadran = None
                    rank = None

                    for extension in point.extensions:
                        if extension.tag == 'pid':
                            pid = int(extension.text)  # Convert to integer
                        elif extension.tag == 'kuadran':
                            kuadran = extension.text
                        elif extension.tag == 'rank':
                            rank = extension.text

                    # Append the object with additional keys
                    elevation_profile.append({
                        'id': i,
                        'pid': pid,
                        'kuadran': kuadran,
                        'rank': rank,
                        'h': point.elevation,
                        'lon': point.longitude,
                        'lat': point.latitude,
                    })

        elevation_df = pd.DataFrame(elevation_profile)
        elevation_gdf = gpd.GeoDataFrame(elevation_profile, geometry=gpd.points_from_xy(
        elevation_df.lon, elevation_df.lat), crs='epsg:4326')
        elevation_gdf = elevation_gdf.drop(['lon', 'lat'], axis=1)
        elevation_gdf = elevation_gdf.to_crs('epsg:3857')
        elevation_gdf['distance'] = elevation_gdf.distance(gdf_site_loc.iloc[0]['geometry'])
        elevation_gdf['distance'] = round(elevation_gdf['distance'])
        elevation_gdf['hv'] = elevation_gdf['h']+vegetation_height
        elevation_gdf = elevation_gdf.to_crs('epsg:4326')
        elevation_gdf['rank'] = elevation_gdf['rank'].astype(int)
        elevation_gdf['kuadran'] = elevation_gdf['kuadran'].astype(int)
        site_hg = elevation_gdf.iloc[0]['h']
        nearest_opt_site['start_hg'] = site_hg
        site_hv = site_hg+vegetation_height
        nearest_opt_site['start_hv'] = site_hv
        site_ht = site_hg+tower_height
        nearest_opt_site['start_ht'] = site_ht

        elevation_sorted = elevation_gdf.copy()
        elevation_sorted = elevation_sorted.sort_values(by=['distance'], ascending=False)
        elevation_sorted = elevation_sorted.drop_duplicates(subset=['kuadran','rank'], keep='first').reset_index(drop=True)
        elevation_sorted = elevation_sorted.sort_values(by=['kuadran','rank'], ascending=True)
        elevation_sorted['kuadran'] = elevation_sorted['kuadran'].astype(int)
        elevation_sorted['rank'] = elevation_sorted['rank'].astype(int)
        elevation_sorted['end_hg'] = elevation_sorted['h'].astype(float)

        nearest_opt_site = pd.merge(nearest_opt_site, elevation_sorted[['kuadran','rank','end_hg']], on=['kuadran','rank'], how='inner')
        nearest_opt_site['end_hv'] = nearest_opt_site['end_hg']+vegetation_height
        nearest_opt_site['end_ht'] = nearest_opt_site['end_hg']+nearest_opt_site['tower_height']
        
        for i, row in nearest_opt_site.iterrows():
            if row['start_ht'] > row['end_ht']:
                nearest_opt_site.loc[i, 'type'] = "down"
                dht = row['start_ht'] - row['end_ht']
                nearest_opt_site.at[i, 'dht'] = dht
                alpha = sudut_antara_a_dan_c(row['distance'], dht)
                nearest_opt_site.at[i, 'alpha'] = alpha
                # sisi_b = float(round(panjang_sisi_b(alpha, row['distance'])))+float(row['end_ht'])
            elif row['start_ht'] == row['end_ht']:
                nearest_opt_site.at[i, 'dht'] = 0
                nearest_opt_site.at[i, 'type'] = "same"
            elif row['start_ht'] < row['end_ht']:
                nearest_opt_site.at[i, 'type'] = "up"
                dht = row['end_ht'] - row['start_ht']
                nearest_opt_site.at[i, 'dht'] = dht
                alpha = sudut_antara_a_dan_c(row['distance'], dht)
                nearest_opt_site.at[i, 'alpha'] = alpha
                # sisi_b = float(round(panjang_sisi_b(alpha, row['distance'])))+float(row['start_ht'])
        
        
        # Define a function to calculate 'hd'
        def calculate_hd(row, elrow):
            if row['type'] == 'up':
                return float(round(panjang_sisi_b(row['alpha'], elrow['distance']))) + float(row['start_ht'])
            elif row['type'] == 'down':
                return float(round(panjang_sisi_b(row['alpha'], row['distance'] - elrow['distance']))) + float(row['end_ht'])
            elif row['type'] == 'same':
                return float(row['start_ht'])

        # Iterate through the rows
        for i, row in nearest_opt_site.iterrows():
            for j, elrow in elevation_gdf.iterrows():
                if row['kuadran'] == elrow['kuadran'] and row['rank'] == elrow['rank']:
                    hd = calculate_hd(row, elrow)
                    elevation_gdf.at[j, 'hd'] = hd
                    elevation_gdf.at[j, 'status'] = 1 if elrow['hv'] < hd else 0

        elevation_gdf_summary = elevation_gdf.sort_values(by=['kuadran','rank','distance','status'], ascending=[True,True,True,True]).reset_index(drop=True)
        elevation_gdf_summary = elevation_gdf_summary.drop_duplicates(subset=['kuadran','rank','status'], keep='first')
        elevation_gdf_summary = elevation_gdf_summary.sort_values(by=['kuadran','rank','distance','status'], ascending=[True, True, False,False]).reset_index(drop=True)
        summary = elevation_gdf_summary.drop_duplicates(subset=['kuadran'], keep='first')

        if max(summary['status']) == 1:
            site_los_status = 1
        else:
            site_los_status = 0        

        summary = summary.rename(columns={'distance': 'obstacle_distance'})
        los_summary = pd.merge(summary[['kuadran','rank','obstacle_distance','status']], nearest_opt_site[['kuadran','rank','distance']], on=['kuadran','rank'], how='left').reset_index(drop=True)
        los_summary_json = json.loads(los_summary[['kuadran','rank','status']].to_json(orient='records'))

        
        elevation_df_summary = pd.DataFrame()

        elevation_gdf = elevation_gdf.drop(['geometry'], axis=1)
        
        for i, row in los_summary.iterrows():
            for j, elrow in elevation_gdf.iterrows():
                if row['kuadran'] == elrow['kuadran'] and row['rank'] == elrow['rank']:
                    if row['status'] == 0 and elrow['distance'] > row['obstacle_distance']:
                        elevation_gdf.at[j, 'status'] = 0   
                    
                    df_el_row = pd.DataFrame([elrow])
                    elevation_df_summary = pd.concat([elevation_df_summary, df_el_row], ignore_index=True)

        elevation_gdf_json = elevation_df_summary.to_json(orient='records')
        elevation_gdf_json = json.loads(elevation_gdf_json)
    
        return {
            'site_los_status' : site_los_status,
            'summary' : los_summary_json,
            'detail' : elevation_gdf_json
        }

    except Exception as e:
        return {
            'site_los_status' : 0,
            'summary' : None,
            'detail' : None
        }

async def genComidCity2500m(site_id):
    query_city = text(f"""
            SELECT DISTINCT comid_city_2500m
            FROM 
                m_colopriming
            WHERE
                site_id = '{site_id}';
        """)
    async with SessionLocal() as session:
        city = await session.execute(query_city)
        city = city.fetchall()
    
    city = pd.DataFrame(city)
    comid_city_2500m = city['comid_city_2500m'].iloc[0]

    return comid_city_2500m

async def getComidCity2500m(site_loc):
    query_city = text(f"""
            SELECT DISTINCT comid_city
            FROM 
                m_city AS city
            WHERE
                ST_Intersects(
                    ST_Transform(city.geometry, 3857),
                    ST_Buffer(
                        ST_Transform(
                            ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326),
                            3857
                        ),2500
                    )
                );
        """)
    async with SessionLocal() as session:
        city = await session.execute(query_city)
        city = city.fetchall()
    
    city = pd.DataFrame(city)
    comid_city_2500m = list(city['comid_city'])

    return comid_city_2500m

async def getSiroStatus(site_id, operator):
    query = text(f"""
            SELECT *
            FROM 
                m_siro AS siro
            WHERE
                siro.site_id = '{site_id}'
                AND siro.tenant = '{operator}'
        """)
    async with SessionLocal() as session:
        result = await session.execute(query)
        result = result.fetchall()
    
    result = pd.DataFrame(result)

    return result

    
    
async def genComidCity8000m(site_loc):
    query_city = text(f"""
            SELECT DISTINCT comid_city
            FROM 
                m_city AS city
            WHERE
                ST_Intersects(
                    ST_Transform(city.geometry, 3857),
                    ST_Buffer(
                        ST_Transform(
                            ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326),
                            3857
                        ),8000
                    )
                );
        """)
    async with SessionLocal() as session:
        city = await session.execute(query_city)
        city = city.fetchall()
    
    city = pd.DataFrame(city)
    comid_city_8000m = list(city['comid_city'])

    return comid_city_8000m

async def getVillageHcPdrb(site_loc):
    async with SessionLocal() as session:
            query_village = text(f"""
                                SELECT *
                                FROM m_village as village
                                WHERE
                                ST_Intersects(village.geometry, ST_GeomFromText('{site_loc}', 4326)
                                )
                                """)
            
            # 2. village
            village = await session.execute(query_village)
            village = village.fetchall()
            village = pd.DataFrame(village)
            village['geometry'] = village['geometry'].apply(lambda x: wkb.loads(x))
            village = gpd.GeoDataFrame(village, geometry='geometry', crs='EPSG:4326')


            # 3. household_consumption
            household_consumption = village['household_consumption'].values[0]
            
            # 4 PDRB
            pdrb = {"monthly":village['monthly_pdrb'].values[0], 
                    "annually":village['annually_pdrb'].values[0]}

            # 5 District
            comid_district = village['comid_district'].values[0]

    return [village, household_consumption, pdrb, comid_district]
    
async def districtCompetition(site_loc, comid_district):
    async with SessionLocal() as session:
        query_district = text(f"""
                            SELECT
                                comid_district,
                                ST_Union(geometry) AS dissolved_geometry
                            FROM m_village as village
                            WHERE
                                comid_district = '{comid_district}'
                            GROUP BY
                                comid_district;
                            """)

        result_district = await session.execute(query_district)
        district = result_district.fetchall()
        district = pd.DataFrame(district, columns=['comid_district', 'dissolved_geometry'])
        district_geometry = gpd.GeoSeries.from_wkb(district['dissolved_geometry']).values[0]

        try:
            query_district_competition = text(f"""
                                    SELECT 
                                        opt.*,
                                        ST_Distance(
                                            ST_Transform(ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326), 3857),
                                            ST_Transform(opt.geometry, 3857)
                                        ) AS distance
                                    FROM m_operator AS opt
                                    WHERE
                                    ST_Intersects(opt.geometry, ST_GeomFromText('{district_geometry}', 4326))
                                    """)

            result_district_competition = await session.execute(query_district_competition)
            district_competition = result_district_competition.fetchall()
            district_competition = pd.DataFrame(district_competition)

            district_competition = district_competition.groupby('operator').size().reset_index(name="count")
            # district_competition_json = district_competition.to_json(orient='records')
            district_competition_json = {"name":"operator"}
            for i, row in district_competition.iterrows():
                district_competition_json[row['operator']] = row['count']
            district_competition_json = json.dumps([district_competition_json], indent=4)

            operator_list = sorted(list(set(district_competition['operator'].unique())))

            return [district_competition, district_competition_json, operator_list, district_geometry]
        except Exception as e:
            return [None, None, None, district_geometry]

    
def strObjToJson(strObj):
    return json.loads(strObj.replace("'",'"'))


async def  getVillageMarketShare(site_loc, village):
    try:
        village_buffer_geometry = village['geometry'].iloc[0]

        query = text(f"""
            SELECT 
                opt.site_id,
                opt.operator,
                opt.actual_revenue,
                ST_Distance(
                        ST_Transform(ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326), 3857),
                        ST_Transform(opt.geometry, 3857)
                    ) AS distance
            FROM m_operator as opt
            WHERE ST_Intersects(opt.geometry, ST_GeomFromText('{village_buffer_geometry}', 4326))
        """)

        
        async with SessionLocal() as session:
            village_ms = await session.execute(query)
            village_ms = village_ms.fetchall()
            village_ms = pd.DataFrame(village_ms)
            village_ms = village_ms[village_ms['actual_revenue']>0].reset_index(drop=True)
            village_ms = village_ms.groupby('operator').agg(count=('operator', 'size'), actual_revenue=('actual_revenue', 'mean')).reset_index()
        
        return village_ms
    except Exception as e:
        return {}

def getVillageMarketShareOperator(village_ms, operator):
    
    missing_opt_actual_revenue = min(list(village_ms['actual_revenue'])) / 2
        
    opt_list = ["TSEL", "XL", "IOH", "SF"]
    village_opt_list = list(village_ms['operator'])
    missing_opt = [item for item in opt_list if item not in village_opt_list]

    if missing_opt:
        new_rows = []

        for opt in missing_opt:
            new_row = {
                'operator': opt,
                'actual_revenue': missing_opt_actual_revenue if opt == operator else 0,
                'count': 0,
                'ratio': 0
            }
            new_rows.append(new_row)

        if new_rows:
            village_ms = pd.concat([village_ms, pd.DataFrame(new_rows)], ignore_index=True)
    
    total_actual_revenue = village_ms['actual_revenue'].sum()
    village_ms['ratio'] = village_ms['actual_revenue'] / total_actual_revenue

    ms = {}
    for x, row in village_ms.iterrows():
        ms[row['operator']] = row['ratio']

    return ms

async def getMarketShareSurrounding(site_loc, surrounding_sectoral, operator):
    nearestSurrounding = min(surrounding_sectoral['distance'])
    if nearestSurrounding < 1000:
        surroundingBuffer = 500
    else:
        surroundingBuffer = 1000
    
    query = text(
        f"""
        SELECT  opt.*,
                ST_Distance(
                    ST_Transform(ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326), 3857),
                    ST_Transform(opt.geometry, 3857)
                ) AS distance
        FROM m_operator as opt
        WHERE
            ST_Intersects(
                ST_Buffer(
                    ST_Transform(
                        ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326),
                        3857
                    ),
                    {surroundingBuffer}
                ),
                ST_Transform(opt.geometry, 3857)
            );
        """
    )

    async with SessionLocal() as session:
        surroundingMs = await session.execute(query)
        surroundingMs = surroundingMs.fetchall()
        surroundingMs = pd.DataFrame(surroundingMs)
        surroundingMs = surroundingMs[surroundingMs['actual_revenue']>0].reset_index(drop=True)
        surroundingMs = surroundingMs.groupby('operator').agg(count=('operator', 'size'), actual_revenue=('actual_revenue', 'mean')).reset_index()
    
    missing_opt_actual_revenue = min(list(surroundingMs['actual_revenue'])) / 2
        
    opt_list = ["TSEL", "XL", "IOH", "SF"]
    surrounding_opt_list = list(surroundingMs['operator'])
    missing_opt = [item for item in opt_list if item not in surrounding_opt_list]

    if missing_opt:
        new_rows = []

        for opt in missing_opt:
            new_row = {
                'operator': opt,
                'actual_revenue': missing_opt_actual_revenue if opt == operator else 0,
                'count': 0,
                'ratio': 0
            }
            new_rows.append(new_row)

        if new_rows:
            surroundingMs = pd.concat([surroundingMs, pd.DataFrame(new_rows)], ignore_index=True)
    
    total_actual_revenue = surroundingMs['actual_revenue'].sum()
    surroundingMs['ratio'] = surroundingMs['actual_revenue'] / total_actual_revenue

    ms = surroundingMs.set_index('operator')['ratio'].to_dict()
    return ms

def getSiteMarketShare(opt_tenant, revenue):
    siteMarketShare = {}
    
    for i in revenue:
        ms = [i]
        for j in opt_tenant:
            if j != i['operator']:
                tenant = [item for item in revenue if item['operator'] == j][0]
                ms.append(tenant)

        ms = pd.DataFrame(ms)
        ms['ratio'] = round(ms['total_revenue']/sum(ms['total_revenue']),2)
        # ms = ms.to_dict(orient='records')
        msSite = {}
        for x, row in ms.iterrows():
            msSite[row['operator']] = row['ratio']

        msSite = {key: msSite[key] for key in sorted(msSite)}
        siteMarketShare[i['operator']] = msSite

        
    sorted_data = {key: siteMarketShare[key] for key in sorted(siteMarketShare)}
    return sorted_data


async def districtMarketShare(district_geometry):
    try:
        query = text(f"""
                SELECT 
                    opt.site_id,
                    opt.operator,
                    opt.actual_revenue
                FROM m_operator as opt
                WHERE ST_Intersects(opt.geometry, ST_GeomFromText('{district_geometry}', 4326))
            """)
        
        async with SessionLocal() as session:
            district_ms = await session.execute(query)
            district_ms = district_ms.fetchall()
            district_ms = pd.DataFrame(district_ms)
            
        district_ms = district_ms[district_ms['actual_revenue']>0].reset_index(drop=True)
        district_ms = district_ms.groupby('operator').agg(count=('operator', 'size'), actual_revenue=('actual_revenue', 'sum')).reset_index()

        missing_opt_actual_revenue = min(list(district_ms['actual_revenue']))/2
            
        opt_list = ["TSEL","XL","IOH","SF"]

        village_opt_list = list(district_ms['operator'])

        missing_opt = [item for item in opt_list if item not in village_opt_list]

        if len(missing_opt) > 0:
            for i, opt in enumerate(missing_opt):
                last_id = district_ms.index[-1]
                district_ms.at[last_id+1,'operator'] = opt
                district_ms.at[last_id+1,'actual_revenue'] = missing_opt_actual_revenue
                district_ms.at[last_id+1,'count'] = 0
                district_ms.at[last_id+1,'ratio'] = 0
                
        total_actual_revenue = sum(district_ms['actual_revenue'])
        district_ms['ratio'] = district_ms['actual_revenue']/total_actual_revenue

        ms = {}
        for j, row in district_ms.iterrows():
            ms[row['operator']] = row['ratio']

        return ms
    except Exception as e:
        return {"TSEL":1,
                "XL":1,
                "IOH":1,
                "SF":1,
                }

async def surroundingCompetition(site_loc):
    query_competition_profile = text(
        f"""
        SELECT  opt.*,
                ST_Distance(
                    ST_Transform(ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326), 3857),
                    ST_Transform(opt.geometry, 3857)
                ) AS distance
        FROM m_operator as opt
        WHERE
            ST_Intersects(
                ST_Buffer(
                    ST_Transform(
                        ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326),
                        3857
                    ),
                    2500
                ),
                ST_Transform(opt.geometry, 3857)
            );
        """
    )
    async with SessionLocal() as session:
        competition_profile = await session.execute(query_competition_profile)
        competition_profile = competition_profile.fetchall()
    
    competition_profile = pd.DataFrame(competition_profile)
    if len(competition_profile) >0:
        distance_thresholds = [500, 1000, 1500, 2000, 2500]
        column_names = [f'{dist} m' for dist in distance_thresholds]
        opt_list = ["TSEL","XL","IOH","SF"]

        merge_df = pd.DataFrame()

        for dist, col_name in zip(distance_thresholds, column_names):
            arr = []
            for opt in opt_list:
                obj = {"operator": opt, col_name: 0}
                obj[col_name] = len(competition_profile[(competition_profile['distance'] <= dist) & (competition_profile['operator'] == opt)].reset_index(drop=True))
                arr.append(obj)

            df = pd.DataFrame(arr)
            
            if merge_df.empty:
                merge_df = df
            else:
                merge_df = pd.merge(merge_df, df, on='operator', how='outer')
        
        # # Initialize the final transformed data
        json_result = []

        # Iterate through each distance key
        for key in column_names:
            # Initialize a new dictionary for the current distance key
            new_dict = {"name": key}
            # Add the counts for each operator
            for i, entry in merge_df.iterrows():
                new_dict[entry["operator"]] = entry[key]
            json_result.append(new_dict)

        json_result = json.dumps(json_result, indent=4)
        return json_result
    else:
        return None


def getPopulation (site_loc, city_id, coverage):
    city_population = gpd.read_parquet(f"./data/populations/{city_id}.parquet")
    
    buffer = gpd.GeoDataFrame(geometry=[site_loc], crs="epsg:4326")
    buffer = buffer.to_crs('epsg:3857')
    buffer['geometry'] = buffer['geometry'].buffer(8000)
    buffer = buffer.to_crs('epsg:4326')
    
    city_population_coverage = gpd.overlay(city_population, coverage[["geometry"]], how='intersection')
    city_population_8000m = gpd.overlay(city_population, buffer, how='intersection')
    return city_population_coverage, city_population_8000m
# Asynchronous wrapper function
async def get_population(site_loc, city_id, coverage):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        result = await loop.run_in_executor(pool, getPopulation, site_loc, city_id, coverage)
    return result

# Asynchronous main function to process population data
async def process_population_data(site_loc, comid_city_8000m, elevation_profile):
    initial_data = {
        'population': gpd.GeoDataFrame(geometry=[], crs='EPSG:4326'),
        'population_8000m': gpd.GeoDataFrame(geometry=[], crs='EPSG:4326')
    }
    
    tasks = []
    for cid in comid_city_8000m:
        tasks.append(asyncio.create_task(get_population(site_loc, cid, elevation_profile[0])))

    results = await asyncio.gather(*tasks)

    for city_population_coverage, city_population_8000m in results:
        initial_data['population'] = pd.concat([initial_data['population'], city_population_coverage], ignore_index=True)
        initial_data['population_8000m'] = pd.concat([initial_data['population_8000m'], city_population_8000m], ignore_index=True)
    
    total_coverage_population = initial_data['population']['population'].sum()
    initial_data['total_coverage_population'] = total_coverage_population
    
    return initial_data.values()

    

async def getRwi(city_id, coverageGeometry):
    try:
        async with SessionLocal() as session:
            query_rwi = text(f"""
                        SELECT rwi
                        FROM m_rwi
                        WHERE
                        comid_city IN ({city_id})
                        AND ST_Intersects(m_rwi.geometry, ST_GeomFromText('{coverageGeometry}', 4326)
                        )
                        """)
            
            rwi = await session.execute(query_rwi)
            rwi = rwi.fetchall()
            rwi = pd.DataFrame(rwi)

            obj = {}
            obj['min'] = min(rwi['rwi'])
            obj['max'] = max(rwi['rwi'])
            
        return obj
    except:
        return {
            'min': 0,
            'max': 0
            }

async def getInternetSpeedTest(city_id, coverageGeometry):
    initial_data = {
        "m_mobile_internet_speed":{
            'avg_d_kbps': 0,
            'avg_u_kbps': 0,
            'avg_lat_ms': 0,
            'total_devices': 0,
            'total_tests': 0
        }, 
        "m_fixed_internet_speed": {
            'avg_d_kbps': 0,
            'avg_u_kbps': 0,
            'avg_lat_ms': 0,
            'total_devices': 0,
            'total_tests': 0
        }
    }


    for key in initial_data.keys():
        try:
            query_speed_test = text(f"""
                                        SELECT *
                                        FROM {key}
                                        WHERE
                                        comid_city IN ({city_id})
                                        AND ST_Intersects({key}.geometry, ST_GeomFromText('{coverageGeometry}', 4326)
                                        )
                                        """)
            async with SessionLocal() as session:
                        
                speed_test = await session.execute(query_speed_test)
                speed_test = speed_test.fetchall()
                speed_test = pd.DataFrame(speed_test)
                result = speed_test.agg({
                    'avg_d_kbps': 'mean',
                    'avg_u_kbps': 'mean',
                    'avg_lat_ms': 'mean',
                    'devices': 'sum',
                    'tests': 'sum',
                })

            result = result.to_json()
            result_obj = json.loads(result)
            result_obj['total_devices'] = result_obj.pop('devices')
            result_obj['total_tests'] = result_obj.pop('tests')
            
            initial_data[key] = result_obj

        except Exception as e:
            pass
    
    return initial_data

def getSignalLevelCity(city_id, coverage):
    signal_level_city = gpd.read_parquet(f"./data/signal_levels/{city_id}.parquet")
    signal_level_city = signal_level_city.to_crs("epsg:4326")
    signal_level_city = gpd.overlay(signal_level_city, coverage[["geometry"]], how='intersection')
    return signal_level_city

def getBuildingData(city_id, coverage):
    building = gpd.read_parquet(f"./data/buildings/{city_id}.parquet")
    building = building.to_crs("epsg:4326")
    building['geom'] = building['geometry']
    building = building.set_geometry('centroid')
    building = building.to_crs('epsg:4326')
    coverage = coverage.to_crs('epsg:4326')
    building = gpd.overlay(building, coverage[["geometry"]], how='intersection').reset_index(drop=True)
    building['centroid'] = building['geometry']
    building['geometry'] = building['geom']
    building = building.set_geometry('geometry')
    building = building.drop(['geom'], axis=1)
    building = building.to_crs('epsg:3857')
    building['area'] = building['geometry'].area
    building = building.to_crs('epsg:4326')
    return building
def getPoiData(site_loc, city_id, coverage):
    data = gpd.read_parquet(f"./data/pois/{city_id}.parquet")
    data = data.to_crs("epsg:4326")
    data = gpd.overlay(data, coverage[["geometry"]], how='intersection').reset_index(drop=True)

    site_gdf = gpd.GeoDataFrame(geometry=[site_loc], crs='epsg:4326')
    site_gdf = site_gdf.to_crs('epsg:3857')
    site_loc = site_gdf['geometry'].iloc[0]
    data = data.to_crs('epsg:3857')
    data['distance'] = data.distance(site_loc)
    data = data.to_crs('epsg:4326')
    return data

def getRoadData(city_id, coverage):
    coverage = coverage.to_crs('epsg:4326')
    road = gpd.read_parquet(f"./data/roads/{city_id}.parquet")
    road = road.set_geometry('geometry')
    road = road.to_crs('epsg:4326')
    road = gpd.overlay(road, coverage[["geometry"]], how='intersection')

    return road
    
    # road_line = road.copy()
    # road_line = road_line.drop(['buffer'], axis=1)
    # road_line = road_line.set_geometry('geometry')

    # road_line.to_parquet(f"./road_{city_id}.parquet")

    # road_buffer = road.copy()
    # road_buffer = road_buffer.set_geometry('buffer')
    # road_buffer['dis'] = 1
    # road_buffer = road_buffer.to_crs('epsg:4326')
    # road_buffer = road_buffer.dissolve(by='dis').reset_index(drop=True)
    # road_buffer = gpd.overlay(road_buffer, coverage[["geometry"]], how='intersection')

    # single_road = road_line.copy()
    # single_road = single_road.to_crs('epsg:3857')
    # single_road = single_road.explode(index_parts=False).reset_index(drop=True)
    # single_road = single_road.to_crs('epsg:3857')

    # try:
    #     osm_graph = momepy.gdf_to_nx(single_road)
    #     osm_graph.graph["crs"] = single_road.crs

    #     streets = ox.graph_to_gdfs(
    #         osm_graph,
    #         nodes=False,
    #         edges=True,
    #         node_geometry=False,
    #         fill_edge_geometry=True
    #     )

    #     streets = momepy.remove_false_nodes(streets)
    #     streets = streets[["geometry"]]
    #     streets["nID"] = range(len(streets))
    #     streets = streets.to_crs('epsg:3857')
    #     streets['length'] = streets.length

    #     graphLocal = momepy.gdf_to_nx(streets)
    #     graphLocal = momepy.closeness_centrality(graphLocal, name='closeness', radius=400, distance='mm_len', weight='mm_len')

    #     nodesLocal, streetsLocal = momepy.nx_to_gdf(graphLocal)
    #     nodesLocal = nodesLocal.to_crs("epsg:4326")
    # except:
    #     nodesLocal = gpd.GeoDataFrame()

    # return road_line, road_buffer, nodesLocal



# Asynchronous wrapper functions
async def get_signal_level_city(city_id, coverage):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        result = await loop.run_in_executor(pool, getSignalLevelCity, city_id, coverage)
    return result

async def get_building_data(city_id, coverage):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        result = await loop.run_in_executor(pool, getBuildingData, city_id, coverage)
    return result
async def get_poi_data(site_loc, city_id, coverage):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        result = await loop.run_in_executor(pool, getPoiData, site_loc, city_id, coverage)
    return result

async def get_road_data(city_id, coverage):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        result = await loop.run_in_executor(pool, getRoadData, city_id, coverage)
    return result

# Asynchronous main function
async def process_city_data(site_loc, city_ids, elevation_profile):
    initial_data = {
        'signal_level': pd.DataFrame(),
        'signal_level_data': pd.DataFrame(),
        'building': pd.DataFrame(),
        'road_line': pd.DataFrame(),
        'road_buffer': pd.DataFrame(),
        'closeness': pd.DataFrame(),
        'poi':pd.DataFrame()
    }

    road = pd.DataFrame()
    
    tasks = []
    for city_id in city_ids:
        tasks.append(asyncio.create_task(get_signal_level_city(city_id, elevation_profile[0])))
        tasks.append(asyncio.create_task(get_building_data(city_id, elevation_profile[0])))
        tasks.append(asyncio.create_task(get_road_data(city_id, elevation_profile[0])))
        tasks.append(asyncio.create_task(get_poi_data(site_loc, city_id, elevation_profile[0])))
    
    results = await asyncio.gather(*tasks)
    
    for i, city_id in enumerate(city_ids):
        signal_level_city = results[i * 4]
        building_data = results[i * 4 + 1]
        road_data = results[i * 4 + 2]
        poi = results[i * 4 + 3]

        initial_data['signal_level_data'] = pd.concat([initial_data['signal_level_data'], signal_level_city], ignore_index=True)
        initial_data['building'] = pd.concat([initial_data['building'], building_data], ignore_index=True)
        road = pd.concat([road, road_data], ignore_index=True)
        initial_data['poi'] = pd.concat([initial_data['poi'], poi], ignore_index=True)
        # initial_data['road_buffer'] = pd.concat([initial_data['road_buffer'], road_data[1]], ignore_index=True)
        # initial_data['closeness'] = pd.concat([initial_data['closeness'], road_data[2]], ignore_index=True)
    
    coverage_signal_level = initial_data['signal_level_data'].groupby('operator')['avg_rsrp'].mean().reset_index(name="4G")
    coverage_signal_level_json = coverage_signal_level.to_json(orient="records")
    coverage_signal_level_json = json.loads(coverage_signal_level_json)
    coverage_signal_level_json = {item['operator']: round(item['4G'], 1) for item in coverage_signal_level_json}
    
    initial_data['signal_level'] = coverage_signal_level_json

    road_line = road.copy()
    road_line = road_line.drop(['buffer'], axis=1)
    road_line = road_line.set_geometry('geometry')
    initial_data['road_line'] = road_line

    road_buffer = road.copy()
    road_buffer = road_buffer.set_geometry('buffer')
    road_buffer['dis'] = 1
    road_buffer = road_buffer.to_crs('epsg:4326')
    road_buffer = road_buffer.dissolve(by='dis').reset_index(drop=True)
    initial_data['road_buffer'] = road_buffer

    single_road = road_line.copy()
    single_road = single_road.to_crs('epsg:3857')
    single_road = single_road.explode(index_parts=False).reset_index(drop=True)
    single_road = single_road.to_crs('epsg:3857')

    try:
        osm_graph = momepy.gdf_to_nx(single_road)
        osm_graph.graph["crs"] = single_road.crs

        streets = ox.graph_to_gdfs(
            osm_graph,
            nodes=False,
            edges=True,
            node_geometry=False,
            fill_edge_geometry=True
        )

        streets = momepy.remove_false_nodes(streets)
        streets = streets[["geometry"]]
        streets["nID"] = range(len(streets))
        streets = streets.to_crs('epsg:3857')
        streets['length'] = streets.length

        graphLocal = momepy.gdf_to_nx(streets)
        graphLocal = momepy.closeness_centrality(graphLocal, name='closeness', radius=400, distance='mm_len', weight='mm_len')

        nodesLocal, streetsLocal = momepy.nx_to_gdf(graphLocal)
        nodesLocal = nodesLocal.to_crs("epsg:4326")
    except:
        nodesLocal = gpd.GeoDataFrame()
    
    initial_data['closeness'] = nodesLocal

    return initial_data.values()
    # return initial_data
async def getNearestColoprimingSite(site_loc):
    try:
        sectoral = genSectoral(site_loc.x, site_loc.y,  90, 4, 10000)
        query = text(
            f"""
            SELECT 
                colo.site_id,
                colo.site_name,
                colo.tower_height,
                colo.longitude,
                colo.latitude,
                ST_Distance(
                    ST_Transform(ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326), 3857),
                    ST_Transform(colo.geometry, 3857)
                ) AS distance
            FROM m_colopriming AS colo
            WHERE
                ST_Intersects(
                    ST_Transform(colo.geometry, 3857),
                    ST_Buffer(
                        ST_Transform(
                            ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326),
                            3857
                        ),
                        10000
                    )
                )
            """
        )


        async with SessionLocal() as session:       
            colopriming_site = await session.execute(query)
            colopriming_site = colopriming_site.fetchall()
        
        nearest_colopriming_site = pd.DataFrame(colopriming_site)
        nearest_colopriming_site = gpd.GeoDataFrame(nearest_colopriming_site, geometry=gpd.points_from_xy(nearest_colopriming_site.longitude, nearest_colopriming_site.latitude), crs='EPSG:4326')
        nearest_colopriming_site = nearest_colopriming_site[nearest_colopriming_site['distance']>100].reset_index(drop=True)
        nearest_colopriming_site = gpd.overlay(nearest_colopriming_site, sectoral[['kuadran','geometry']], how='intersection')
        nearest_colopriming_site = nearest_colopriming_site.sort_values(by=['kuadran','distance'], ascending=True)
        nearest_colopriming_site = nearest_colopriming_site.drop_duplicates(subset=['kuadran'], keep='first').reset_index(drop=True)
        nearest_colopriming_site = nearest_colopriming_site.drop(['geometry'], axis=1)
        nearest_colopriming_site_json = nearest_colopriming_site.to_json(orient="records")
        nearest_colopriming_site_json = json.loads(nearest_colopriming_site_json)
        
        nearest_colopriming_site_json = {
            "nearest_colopriming": nearest_colopriming_site_json
        }

        return nearest_colopriming_site_json
        
    except Exception as e:
        return {
            "nearest_colopriming": []
        }

def distanceToBuffer(distance, min_distance, towerCoverageRadius):
    
    if min_distance > 1000.0:
        buffer = distance * 0.4
    else:
        buffer = distance * 0.6
    
    if distance == towerCoverageRadius:
        buffer = towerCoverageRadius
    
    return buffer


def genSurroundingSectoral(site_loc, operator, towerCoverageRadius, radiusException):
    sectoral = genSectoral(site_loc.x, site_loc.y,  90, 4, 10000)
    operator_site = gpd.GeoDataFrame()
    for i, row in sectoral.iterrows():
        query = text(f"""
            SELECT 
                opt.site_id,
                opt.site_name,
                opt.tower_height,
                opt.actual_revenue,
                opt.longitude,
                opt.latitude,
                ST_Distance(
                    ST_Transform(ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326), 3857),
                    ST_Transform(opt.geometry, 3857)
                ) AS distance
            FROM m_operator AS opt
            WHERE
                opt.operator = '{operator}'
                AND ST_Intersects(opt.geometry, ST_GeomFromText('{row['geometry']}', 4326))
                AND ST_Distance(
                    ST_Transform(ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326), 3857),
                    ST_Transform(opt.geometry, 3857)
                ) > {radiusException};
        """)
        with SessionSync() as session:
            opt_site = session.execute(query)
            opt_site = opt_site.fetchall()
            opt_site = pd.DataFrame(opt_site)
        
        if len(opt_site)>0:
            opt_site['kuadran'] = row['kuadran']
        else:
            opt_site = pd.DataFrame([{
                "site_id": f"sudo_kudaran_{row['kuadran']}",
                "site_name": f"sudo_kudaran_{row['kuadran']}",
                "tower_height": 0,
                "longitude":row['center_vertices'][0],
                "latitude":row['center_vertices'][1],
                "distance": towerCoverageRadius,
                "kuadran": row['kuadran']
                }])
        
        operator_site = pd.concat([operator_site, opt_site], ignore_index=True)
    
        
    surrounding_operator_site = operator_site
    surrounding_operator_site = surrounding_operator_site.sort_values(by=['distance'], ascending=True)
    surrounding_operator_site = surrounding_operator_site.drop_duplicates(subset=['kuadran'], keep='first').reset_index(drop=True)
    surrounding_operator_site = surrounding_operator_site.sort_values(by=['kuadran'], ascending=True)

    klist = surrounding_operator_site['kuadran'].to_list()
    missing_k = [item for item in range(1,5) if item not in klist]

    if len(missing_k) > 0:
        for k in missing_k:
            sudo_k = pd.DataFrame([{
                "site_id": f"sudo_kudaran_{k}",
                "site_name": f"sudo_kudaran_{k}",
                "tower_height": 0,
                "longitude":list(sectoral[sectoral['kuadran'] == k]['center_vertices'])[0][0],
                "latitude":list(sectoral[sectoral['kuadran'] == k]['center_vertices'])[0][1],
                "distance": towerCoverageRadius,
                "kuadran": k
                }])
            surrounding_operator_site = pd.concat([surrounding_operator_site, sudo_k], ignore_index=True)
    
    surrounding_operator_site = surrounding_operator_site.sort_values(by=['distance'], ascending=True)
    minimum_distance = min(list(surrounding_operator_site['distance']))
    surrounding_operator_site['kuadran'] = surrounding_operator_site['kuadran']-1
    surrounding_operator_site['buffer'] = surrounding_operator_site['distance'].apply(lambda distance: distanceToBuffer(distance, minimum_distance, towerCoverageRadius))

    for i, row in surrounding_operator_site.iterrows():
        if row['buffer'] > towerCoverageRadius:
            surrounding_operator_site.at[i, 'buffer'] = towerCoverageRadius

    surrounding_operator_site_json = surrounding_operator_site.to_json(orient="records")
    surrounding_operator_site_json = json.loads(surrounding_operator_site_json)

    return surrounding_operator_site



def genSurroundingSectoralSiro(site_loc, operator, towerCoverageRadius, radiusException):
    sectoral = genSectoral(site_loc.x, site_loc.y,  90, 4, 10000)
    operator_site = gpd.GeoDataFrame()
    for i, row in sectoral.iterrows():
        query = text(f"""
            SELECT 
                opt.site_id,
                opt.site_name,
                opt.tower_height,
                opt.actual_revenue,
                opt.longitude,
                opt.latitude,
                ST_Distance(
                    ST_Transform(ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326), 3857),
                    ST_Transform(opt.geometry, 3857)
                ) AS distance
            FROM m_operator AS opt
            WHERE
                opt.operator = '{operator}'
                AND ST_Intersects(opt.geometry, ST_GeomFromText('{row['geometry']}', 4326))
                AND ST_Distance(
                    ST_Transform(ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326), 3857),
                    ST_Transform(opt.geometry, 3857)
                ) > {radiusException};
        """)

        with SessionSync() as session:
            opt_site = session.execute(query)
            opt_site = opt_site.fetchall()
            opt_site = pd.DataFrame(opt_site)
        
        if len(opt_site)>0:
            opt_site['kuadran'] = row['kuadran']
        else:
            opt_site = pd.DataFrame([{
                "site_id": f"sudo_kudaran_{row['kuadran']}",
                "site_name": f"sudo_kudaran_{row['kuadran']}",
                "tower_height": 0,
                "longitude":row['center_vertices'][0],
                "latitude":row['center_vertices'][1],
                "distance": towerCoverageRadius,
                "kuadran": row['kuadran']
                }])
        
        operator_site = pd.concat([operator_site, opt_site], ignore_index=True)
    
        
    surrounding_operator_site = operator_site
    surrounding_operator_site = surrounding_operator_site.sort_values(by=['distance'], ascending=True)
    surrounding_operator_site = surrounding_operator_site.drop_duplicates(subset=['kuadran'], keep='first').reset_index(drop=True)
    surrounding_operator_site = surrounding_operator_site.sort_values(by=['kuadran'], ascending=True)

    klist = surrounding_operator_site['kuadran'].to_list()
    missing_k = [item for item in range(1,5) if item not in klist]

    if len(missing_k) > 0:
        for k in missing_k:
            sudo_k = pd.DataFrame([{
                "site_id": f"sudo_kudaran_{k}",
                "site_name": f"sudo_kudaran_{k}",
                "tower_height": 0,
                "longitude":list(sectoral[sectoral['kuadran'] == k]['center_vertices'])[0][0],
                "latitude":list(sectoral[sectoral['kuadran'] == k]['center_vertices'])[0][1],
                "distance": towerCoverageRadius,
                "kuadran": k
                }])
            surrounding_operator_site = pd.concat([surrounding_operator_site, sudo_k], ignore_index=True)
    
    surrounding_operator_site = surrounding_operator_site.sort_values(by=['distance'], ascending=True)
    minimum_distance = min(list(surrounding_operator_site['distance']))
    surrounding_operator_site['kuadran'] = surrounding_operator_site['kuadran']-1
    surrounding_operator_site['buffer'] = surrounding_operator_site['distance'].apply(lambda distance: distanceToBuffer(distance, minimum_distance, towerCoverageRadius))

    for i, row in surrounding_operator_site.iterrows():
        if row['buffer'] > towerCoverageRadius:
            surrounding_operator_site.at[i, 'buffer'] = towerCoverageRadius

    surrounding_operator_site_json = surrounding_operator_site.to_json(orient="records")
    surrounding_operator_site_json = json.loads(surrounding_operator_site_json)

    return surrounding_operator_site


def gradingSurrounding(site_loc, operator, towerCoverageRadius, radiusException):
    sectoral = genSectoral(site_loc.x, site_loc.y,  90, 4, 10000)
    operator_site = gpd.GeoDataFrame()
    for i, row in sectoral.iterrows():
        query = text(f"""
            SELECT 
                opt.site_id,
                opt.site_name,
                opt.tower_height,
                opt.actual_revenue,
                opt.longitude,
                opt.latitude,
                ST_Distance(
                    ST_Transform(ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326), 3857),
                    ST_Transform(opt.geometry, 3857)
                ) AS distance
            FROM m_operator AS opt
            WHERE
                opt.operator = '{operator}'
                AND ST_Intersects(opt.geometry, ST_GeomFromText('{row['geometry']}', 4326))
                AND ST_Distance(
                    ST_Transform(ST_SetSRID(ST_GeomFromText('{site_loc}'), 4326), 3857),
                    ST_Transform(opt.geometry, 3857)
                ) > {radiusException};
        """)
        with SessionSync() as session:
            opt_site = session.execute(query)
            opt_site = opt_site.fetchall()
            opt_site = pd.DataFrame(opt_site)
        
        if len(opt_site)>0:
            opt_site['kuadran'] = row['kuadran']
        else:
            opt_site = pd.DataFrame([{
                "site_id": f"sudo_kudaran_{row['kuadran']}",
                "site_name": f"sudo_kudaran_{row['kuadran']}",
                "tower_height": 0,
                "longitude":row['center_vertices'][0],
                "latitude":row['center_vertices'][1],
                "distance": towerCoverageRadius,
                "kuadran": row['kuadran']
                }])
        
        operator_site = pd.concat([operator_site, opt_site], ignore_index=True)
    
        
    surrounding_operator_site = operator_site
    surrounding_operator_site = surrounding_operator_site.sort_values(by=['distance'], ascending=True)
    surrounding_operator_site = surrounding_operator_site.drop_duplicates(subset=['kuadran'], keep='first').reset_index(drop=True)
    surrounding_operator_site = surrounding_operator_site.sort_values(by=['kuadran'], ascending=True)
    surrounding_operator_site = surrounding_operator_site.sort_values(by=['distance'], ascending=True)
    surrounding_operator_site['kuadran'] = surrounding_operator_site['kuadran']-1
    return surrounding_operator_site

def pop_distance(pop, given):
    a = pop-given
    if a < 1 :
        a = a*-1
    return a

def populationSimilarity(data, population):
    df = data.copy()
    
    n_clusters = 2
    
    if len(df) == 1:
        df['classification'] = "similar"
        data['classification'] = df['classification']
        data['population_distance'] = data['total_population'].apply(lambda pop: pop_distance(pop, population))
        data = data[data['classification'] == 'similar'].reset_index(drop=True)
        data = data.head(1)
        return data
    
    features = ['total_population']
    scaler = MinMaxScaler()
    df[features] = scaler.fit_transform(df[features])
    kmeans = KMeans(n_clusters=n_clusters, random_state=0, n_init=1).fit(df[features])
    df['cluster'] = kmeans.labels_
    
    given_population = population
    
    normalized_given_population = scaler.transform(pd.DataFrame({ 'total_population': [given_population] }))[0]

    cluster_centers = kmeans.cluster_centers_
    distances = np.linalg.norm(cluster_centers - normalized_given_population, axis=1)
    closest_cluster = np.argmin(distances)

    df['classification'] = df['cluster'].apply(lambda x: 'similar' if x == closest_cluster else 'not similar')
    
    data['classification'] = df['classification']
    data['population_distance'] = data['total_population'].apply(lambda pop: pop_distance(pop, given_population))
    data = data[data['classification'] == 'similar'].reset_index(drop=True)
    data = data.sort_values(by=['population_distance']).reset_index(drop=True)
    data = data.head(1)
    return data

async def ioSectoral(sectoral, population_8000m, operator):
    tasks = []

    async with asyncio.TaskGroup() as tg:  
        for j, row in sectoral.iterrows():
            task = tg.create_task(process_sectoral_row(row, population_8000m, operator))
            tasks.append(task)

    results = [task.result() for task in tasks]

    for j, total_sectoral_population in enumerate(results):
        sectoral.at[j, 'total_population'] = total_sectoral_population

    return sectoral

async def process_sectoral_row(row, population_8000m, operator):
    site_loc = Point(row['longitude'], row['latitude'])
    towerCoverageRadius = CoverageRadius(row['tower_height'])
    towerCoverageBufferArea = drawBuffer(row['longitude'], row['latitude'], towerCoverageRadius)
    elevationProfile = await sectoralElevationProfile(row['longitude'], row['latitude'], row['tower_height'], 4, 90, 0.5, towerCoverageRadius, towerCoverageBufferArea)
    elevationProfileCoverage = elevationProfile[0]
    surrounding_sectoral = genSurroundingSectoral(site_loc, operator, towerCoverageRadius, 400)
    quadran_sectoral = genQuadranSectoral(site_loc, surrounding_sectoral.copy())
    quadran_sectoral = gpd.overlay(quadran_sectoral, elevationProfileCoverage[["geometry"]], how='intersection', keep_geom_type=True)
    sectoral_population = gpd.overlay(population_8000m, quadran_sectoral[['kuadran', 'geometry']], how='intersection', keep_geom_type=True)
    total_sectoral_population = sectoral_population['population'].sum()
    return total_sectoral_population

def genArpu(operator):
    arpu = 0
    if operator == "TSEL":
        arpu = 40000
    elif operator == "XL":
        arpu = 36000
    elif operator == "IOH":
        arpu = 35000
    elif operator == "SF":
        arpu = 30000
    
    return arpu
def genReverseAreaFactor(tenant_revenue,market_share_competition, arpu, population, operator):
    market_share_opeator = market_share_competition[operator]
    area_factor = tenant_revenue/(market_share_opeator*arpu*population)
    return area_factor


async def genAreaFactor(sectoral, population_8000m, population, operator, arpu):

    try:
        sectoral = sectoral[
        (sectoral['distance'] <= 5000.0) &
        (~sectoral['site_name'].str.contains('sudo_kudaran', na=False)) &
        (sectoral['actual_revenue'].astype(str) != 'nan') &
        (sectoral['actual_revenue'] >= 10000000)
        ].reset_index()
        if len(sectoral) == 0:
            area_factor = 1.0
            return area_factor

        sectoral = await ioSectoral(sectoral, population_8000m, operator)

        sectoral= populationSimilarity(sectoral, population)
        sectoral = sectoral[['site_id', 'site_name', 'tower_height', 'actual_revenue',
        'longitude', 'latitude', 'distance', 'kuadran', 'buffer', 'total_population', 'classification', 'population_distance']]
        site_loc = Point(sectoral['longitude'][0], sectoral['latitude'][0])
        market_share_surrounding = await getMarketShareSurrounding(site_loc, sectoral, operator)

        prediction_revenue = market_share_surrounding[operator]*arpu*sectoral['total_population'].iloc[0]
        area_factor = sectoral['actual_revenue']/prediction_revenue
        area_factor = area_factor.values[0]

        if area_factor > 3.0:
            area_factor = 3.0

    except Exception as e:
        area_factor = 1.0

    return area_factor

async def getColoStatus(site_id):
    try:
        async with SessionLocal() as session:
            query = text(f"""
                        SELECT 
                            colo.site_id,
                            colo.tenant,
                            colo.actual_revenue,
                            colo.antena_height
                        FROM m_colopriming as colo
                        WHERE
                        site_id = '{site_id}'
                        """)
            
            result = await session.execute(query)
            result = result.fetchall()
            result = pd.DataFrame(result)
            tenant = list(strObjToJson(result['tenant'][0])['tenant'])
            tenant_revenue = strObjToJson(result['actual_revenue'][0])
            antena_height = result['antena_height'][0]

            opt_tenant = []
            for t in tenant:
                if tenant_revenue[t]>0:
                    opt_tenant.append(t)
        
        return opt_tenant, tenant_revenue, antena_height
    except Exception as e:
        return [], [], 0

async def averageDistanceOperator(opt_loc, operator):
    try:
        radius = 10000
        query = text(
            f"""
            SELECT 
                opt.site_id,
                opt.tower_height,
                opt.longitude,
                opt.latitude,
                ST_Distance(
                    ST_Transform(ST_SetSRID(ST_GeomFromText('{opt_loc}'), 4326), 3857),
                    ST_Transform(opt.geometry, 3857)
                ) AS distance
            FROM m_operator AS opt
            WHERE
                opt.operator = '{operator}' AND
                ST_Intersects(
                    ST_Transform(opt.geometry, 3857),
                    ST_Buffer(
                        ST_Transform(
                            ST_SetSRID(ST_GeomFromText('{opt_loc}'), 4326),
                            3857
                        ),
                        {radius}
                    )
                )
            """
        )

        async with SessionLocal() as session:       
            nearest_opt = await session.execute(query)
            nearest_opt = nearest_opt.fetchall()
        
        nearest_opt_site = pd.DataFrame(nearest_opt)
        nearest_opt_site = gpd.GeoDataFrame(nearest_opt_site, geometry=gpd.points_from_xy(nearest_opt_site.longitude, nearest_opt_site.latitude), crs='EPSG:4326')
        nearest_opt_site = nearest_opt_site[nearest_opt_site['distance']>100].reset_index(drop=True)
        nearest_opt_site = nearest_opt_site.sort_values(by=['distance']).reset_index(drop=True)[:3]

        d12 = nearest_opt_site.iloc[0]['distance']/nearest_opt_site.iloc[1]['distance']
        d23 = nearest_opt_site.iloc[1]['distance']/nearest_opt_site.iloc[2]['distance']
        
        avg_distance = 0

        if d12>0.7 and d23>0.7:
            avg_distance = np.mean(nearest_opt_site['distance'])
        elif d12>0.7:
            avg_distance = np.mean([nearest_opt_site.iloc[0]['distance'], nearest_opt_site.iloc[1]['distance']])
        else:
            avg_distance = nearest_opt_site.iloc[0]['distance']
        
        return avg_distance
    except Exception as e:
        return 0

async def  colopriming_grading(data, status, revenue_class, signal_strength, operator):
    try:
        if status == 1:
            return "already-colo",'ok',0,0,0

        data = data.sort_values(by=['distance']).reset_index(drop=True)
        if len(data) == 1:
            avg_distance = data['distance'].iloc[0] * 1.2
        elif len(data) == 2:
            d12 = data['distance'].iloc[0] / data['distance'].iloc[1]
            if d12 < 0.7:
                avg_distance = data['distance'].iloc[0]
            else:
                avg_distance = np.mean([data['distance'].iloc[0], data['distance'].iloc[1]])
        elif len(data) == 3:
            d12 = (data['distance'].iloc[0] / data['distance'].iloc[1])
            d23 = (data['distance'].iloc[1] / data['distance'].iloc[2])
            if d12 < 0.6:
                avg_distance = np.mean([data['distance'].iloc[0]])
            if d12 >= 0.6 and d23 < 0.6:
                avg_distance = np.mean([data['distance'].iloc[0], data['distance'].iloc[1]])
            if d12 >= 0.6 and d23 >= 0.6:
                avg_distance = np.mean([data['distance'].iloc[0], data['distance'].iloc[1], data['distance'].iloc[2]])
        elif len(data) == 4:
            d12 = (data['distance'].iloc[0] / data['distance'].iloc[1])
            d23 = (data['distance'].iloc[1] / data['distance'].iloc[2])
            d34 = (data['distance'].iloc[2] / data['distance'].iloc[3])

        if d12 < 0.6:
            avg_distance = np.mean([data['distance'].iloc[0]])
        if d12 >= 0.6 and d23 < 0.6:
            avg_distance = np.mean([data['distance'].iloc[0], data['distance'].iloc[1]])
        if d12 >= 0.6 and d23 >= 0.6 and d34 < 0.6:
            avg_distance = np.mean([data['distance'].iloc[0], data['distance'].iloc[1], data['distance'].iloc[2]])
        if d12 >= 0.6 and d23 >= 0.6 and d34 >= 0.6:
            avg_distance = np.mean([data['distance'].iloc[0], data['distance'].iloc[1], data['distance'].iloc[2], data['distance'].iloc[3]])
        
        surrounding_quality = len(data[data['distance'] <= avg_distance*1.1])

        nearest_opt = data[:1]
        opt_loc = Point(nearest_opt['longitude'][0], nearest_opt['latitude'][0])

        avg_distance_opt = 0
        try:
            avg_distance_opt = await averageDistanceOperator(opt_loc, operator)
        except Exception as e:
            avg_distance_opt = 0

        potential_collocation = 'nok'
        
        if (data['distance'].iloc[0] >= (avg_distance * 0.9 if 250 <= avg_distance < 300 and surrounding_quality > 2 else 300)) or (data['distance'].iloc[0] > 1000 or data['distance'].iloc[0] / avg_distance > 0.4):
            potential_collocation = 'ok'

        if avg_distance_opt == 0:
            if ((data['distance'].iloc[0] < 1000 and surrounding_quality == 1)):
                potential_collocation = 'nok'
        else:
            if ((data['distance'].iloc[0] < 1000 and surrounding_quality == 1 and (data['distance'].iloc[0]/avg_distance_opt) < 0.7)):
                potential_collocation = 'nok'



        sow = "coverage" if data['distance'].iloc[0] > 1000 else "capacity"

        if signal_strength <= -200:
            ss = "poor"
        elif signal_strength <= -105:
            ss = "fair"
        elif signal_strength <= -95:
            ss = "good"
        elif signal_strength < 0:
            ss = "excellent"
        else:
            ss = "poor"
        
        d1 = data['distance'].iloc[0]
        
        if potential_collocation == 'nok':
            revenue_class, sow, ss = None, None, None
            

        grade_map = {
            ('ok', 'PLATINUM', 'coverage', 'poor'): 'signature',
            ('ok', 'PLATINUM', 'coverage', 'fair'): 'signature*',  
            ('ok', 'PLATINUM', 'coverage', 'good'): 'signature*', 
            ('ok', 'PLATINUM', 'coverage', 'exellent'): 'signature*', 

            ('ok', 'PLATINUM', 'capacity', 'poor'): 'prime*', 
            ('ok', 'PLATINUM', 'capacity', 'fair'): 'prime',
            ('ok', 'PLATINUM', 'capacity', 'good'): 'prime',
            ('ok', 'PLATINUM', 'capacity', 'excellent'): 'prime',
            
            ('ok', 'GOLD', 'coverage', 'poor'): 'signature',
            ('ok', 'GOLD', 'coverage', 'fair'): 'signature*',
            ('ok', 'GOLD', 'coverage', 'good'): 'signature*',
            ('ok', 'GOLD', 'coverage', 'excellent'): 'signature*',

            ('ok', 'GOLD', 'capacity', 'poor'): 'prime*',
            ('ok', 'GOLD', 'capacity', 'fair'): 'prime',
            ('ok', 'GOLD', 'capacity', 'good'): 'prime',
            ('ok', 'GOLD', 'capacity', 'excellent'): 'prime',
            
            ('ok', 'SILVER', 'coverage', 'poor'): 'signature',
            ('ok', 'SILVER', 'coverage', 'fair'): 'signature*',
            ('ok', 'SILVER', 'coverage', 'good'): 'signature*',
            ('ok', 'SILVER', 'coverage', 'excellent'): 'signature*',

            ('ok', 'SILVER', 'capacity', 'poor'): 'choice-capacity*',
            ('ok', 'SILVER', 'capacity', 'fair'): 'choice-capacity',
            ('ok', 'SILVER', 'capacity', 'good'): 'choice-capacity',
            ('ok', 'SILVER', 'capacity', 'excellent'): 'choice-capacity',
            
            ('ok', 'BRONZE', 'coverage', 'poor'): 'choice-coverage',
            ('ok', 'BRONZE', 'coverage', 'fair'): 'choice-coverage*',
            ('ok', 'BRONZE', 'coverage', 'good'): 'choice-coverage*',
            ('ok', 'BRONZE', 'coverage', 'excellent'): 'choice-coverage*',

            ('ok', 'BRONZE', 'capacity', 'poor'): 'choice-capacity*',
            ('ok', 'BRONZE', 'capacity', 'fair'): 'choice-capacity',
            ('ok', 'BRONZE', 'capacity', 'good'): 'choice-capacity',
            ('ok', 'BRONZE', 'capacity', 'excellent'): 'choice-capacity',
            ('nok', None, None, None): 'Buffer'
        }

        return grade_map.get((potential_collocation, revenue_class, sow, ss),"-"), potential_collocation, avg_distance, avg_distance_opt, surrounding_quality
    except Exception as e:
        return "-",'nok',0,0,0



def genGridness(quad_distance, operator):
    lenSurrounded, aveRatio = 0, 0
    try:
        quad_distance = quad_distance[quad_distance['distance'] <= 4500].reset_index(drop=True)
        quad_distance = quad_distance.sort_values(by='distance').reset_index(drop=True)
        surrounded = []

        for i, row in quad_distance.iterrows():
            if i <= len(quad_distance)-1:
                try:
                    distance_ratio = (quad_distance['distance'][i]/quad_distance['distance'][i+1])*100
                    if distance_ratio > 70.0 and distance_ratio < 100.0:
                        surrounded.append(quad_distance['kuadran'][i])
                        surrounded.append(quad_distance['kuadran'][i+1])
                    elif distance_ratio <= 70.0 and i == 0:
                        gdf = gpd.GeoDataFrame([row], geometry=gpd.points_from_xy(row['longitude'], row['laitude']), crs='epsg:4326')
                        quad_distance_nested = genSectoral(row['longitude'], row['latitude'], 90, 4, 100000)
                        quad_distance_nested = quad_distance_nested.sort_values(by='distance').reset_index(drop=True)
                        for i, row in quad_distance_nested.iterrows():
                            distance_ratio_nested = (quad_distance_nested['distance'][i]/quad_distance_nested['distance'][i+1])*100
                            if distance_ratio_nested > 70.0:
                                surrounded.append(quad_distance_nested['kuadran'][i])
                                surrounded.append(quad_distance_nested['kuadran'][i+1])
                            else:
                                break
                    else:
                        break
                except:
                    break
        
        if len(surrounded)>0:
            surrounded = list(set(surrounded))
        
        quad_distance = quad_distance[quad_distance['kuadran'].isin(surrounded)].reset_index(drop=True)
        
        kuadran_ratio = []
        aveRatio = 0
        lenSurrounded = 0

        if len(surrounded) > 1:
            if 0 in surrounded and 1 in surrounded:
                d01 = dRatio(0, 1, quad_distance)
                kuadran_ratio.append(d01)
            if 1 in surrounded and 2 in surrounded:
                d12 = dRatio(1, 2, quad_distance)
                kuadran_ratio.append(d12)
            if 2 in surrounded and 3 in surrounded:
                d23 = dRatio(2, 3, quad_distance)
                kuadran_ratio.append(d23)
            if 3 in surrounded and 0 in surrounded:
                d30 = dRatio(3, 0, quad_distance)
                kuadran_ratio.append(d30)
            if 0 in surrounded and 2 in surrounded:
                d02 = dRatio(0, 2, quad_distance)
                kuadran_ratio.append(d02)
            if 1 in surrounded and 3 in surrounded:
                d13 = dRatio(1, 3, quad_distance)
                kuadran_ratio.append(d13)

            if len(kuadran_ratio) > 0:
                aveRatio = sum(kuadran_ratio)/len(kuadran_ratio)
                lenSurrounded = len(surrounded)
                return lenSurrounded, aveRatio
                # obj = {'gridness_quadran': lenSurrounded,'gridness_ratio': aveRatio}
                # return obj

            else:
                return lenSurrounded, aveRatio
                # obj = {'gridness_quadran': 0,'gridness_ratio': 0}
                # return obj

        else:
            return lenSurrounded, aveRatio
    except Exception as e:
        return lenSurrounded, aveRatio
                # obj = {'gridness_quadran': lenSurrounded,'gridness_ratio': aveRatio}
        # return obj

def lineSectoralIdToQuadran(id):
    kuadran = 1
    if id >= 0 and id <= 8:
        kuadran = 1
    elif id >= 9 and id <= 17:
        kuadran = 2
    elif id >= 18 and id <= 26:
        kuadran = 3
    elif id >= 27 and id <= 35:
        kuadran = 4
    elif id >= 36 and id <= 44:
        kuadran = 5
    elif id >= 45 and id <= 53:
        kuadran = 6
    elif id >= 54 and id <= 62:
        kuadran = 7
    elif id >= 63 and id <= 71:
        kuadran = 8
    return kuadran

async def mainOpt(colopriming_site, site_loc, coloTowerCoverageRadius, towerCoverageRadius, elevationProfileCoverage, population, village_market_share, population_8000m,  opt_tenant, tenant_revenue, building_data, road_data, closeness_data, road_line, line_sectoral, sectoral_elevation_profile, poi, signal_level_data,district_ms, isSiro):
    opt_list = colopriming_site['operator']
    tower_height = colopriming_site['tower_height']

    tasks = []

    async with asyncio.TaskGroup() as tg:    
        for i, operator in enumerate(opt_list):
            task = tg.create_task(process_operator(operator, tower_height, site_loc, coloTowerCoverageRadius, towerCoverageRadius, elevationProfileCoverage, population, village_market_share, population_8000m,  opt_tenant, tenant_revenue, building_data, road_data, closeness_data, road_line, line_sectoral, sectoral_elevation_profile, poi, signal_level_data,district_ms, isSiro))
            tasks.append(task)

    recap = [task.result()["recap"] for task in tasks]
    spatial = [task.result()["spatial_result"] for task in tasks]

    return recap, spatial

def determine_coverage(row):
    if row['h'] <= row['tdh'] and row['distance_to_center'] <= row['max_distance']:
        return 1
    else:
        return 0
    

async def process_operator(operator, tower_height, site_loc, coloTowerCoverageRadius, towerCoverageRadius, elevationProfileCoverage, population, village_market_share, population_8000m,  opt_tenant, tenant_revenue, building_data, road_data, closeness_data, road_line, line_sectoral, sectoral_elevation_profile, poi, signal_level_data, district_ms, isSiro):
    try:
        recap= {}
        recap['operator'] = str(operator)

        spatial_result = {operator: {}}

        if isSiro != True:
            losProfile = await genLos(site_loc, tower_height, operator, 15)
        
        ############################### CALCULATE POPULATION BY ACTUAL REVENUE ######################################
        colo_surrounding_sectoral = genSurroundingSectoral(site_loc, operator, coloTowerCoverageRadius, 400)
        colo_quadran_sectoral = genQuadranSectoral(site_loc, colo_surrounding_sectoral)
        colo_quadran_sectoral = gpd.overlay(colo_quadran_sectoral, elevationProfileCoverage[["geometry"]], how='intersection', keep_geom_type=True)
        colo_sectoral_population = gpd.overlay(population, colo_quadran_sectoral[['kuadran','azimuth_start','azimuth_end','geometry']], how='intersection', keep_geom_type=True)
        colo_sectoral_population = colo_sectoral_population.groupby(['kuadran','azimuth_start','azimuth_end'])['population'].sum().reset_index()
        colo_sectoral_population.columns = ['kuadran','azimuth_start','azimuth_end','total_population']
        colo_sectoral_population = colo_sectoral_population['total_population'].sum()
        #############################################################################################################
        surrounding_sectoral = genSurroundingSectoral(site_loc, operator, towerCoverageRadius, 400)
        gridness_quadran, gridness_ratio = genGridness(surrounding_sectoral, operator)
        quadran_sectoral = genQuadranSectoral(site_loc, surrounding_sectoral)
        quadran_sectoral = gpd.overlay(quadran_sectoral, elevationProfileCoverage[["geometry"]], how='intersection', keep_geom_type=True)
        quadran_sectoral = quadran_sectoral.to_crs('epsg:3857')
        quadran_sectoral['area'] = quadran_sectoral['geometry'].area
        quadran_sectoral = quadran_sectoral.to_crs('epsg:4326')
        total_quadran_sectoral_area = quadran_sectoral['area'].sum()
        quadran_sectoral_area = quadran_sectoral[['kuadran','area']]
        quadran_sectoral_area.columns = ['kuadran','sectoral_area_m2']

        spatial_result[operator]["quadran_sectoral"] = quadran_sectoral

        sectoral_population = gpd.overlay(population, quadran_sectoral[['kuadran','azimuth_start','azimuth_end','geometry']], how='intersection', keep_geom_type=True)
        sectoral_population = sectoral_population.groupby(['kuadran','azimuth_start','azimuth_end'])['population'].sum().reset_index()
        sectoral_population.columns = ['kuadran','azimuth_start','azimuth_end','total_population']
        total_sectoral_population = sectoral_population['total_population'].sum()

        # market_share_competition = getVillageMarketShareOperator(village_market_share, operator)
        # try:
        #     market_share_surrounding = await getMarketShareSurrounding(site_loc, surrounding_sectoral, operator)
        # except:
        #     market_share_surrounding = market_share_competition
        
        try:
            market_share_competition = getVillageMarketShareOperator(village_market_share, operator)
        except Exception as e:
            market_share_competition = district_ms

        try:
            market_share_surrounding = await getMarketShareSurrounding(site_loc, surrounding_sectoral, operator)
        except Exception as e:
            market_share_surrounding = market_share_competition
            
        arpu = genArpu(operator)

        sectoral_info = gpd.GeoDataFrame()

        if operator not in opt_tenant:
            # ini kalo operator belum colo di suatu site
            # penentuan area factor berdasarkan site di sekitarnya
            try:
                status = 0
                recap['colopriming_status'] = int(0)
                area_factor = await genAreaFactor(surrounding_sectoral, population_8000m, total_sectoral_population, operator, arpu)
                total_revenue, sectoral_info, grade_revenue, pass_max_actual_revenue = genRevenue(total_sectoral_population, sectoral_population, market_share_surrounding, arpu, area_factor, operator, surrounding_sectoral)
                if pass_max_actual_revenue == 1:
                    area_factor = genReverseAreaFactor(total_revenue, market_share_surrounding, arpu, total_sectoral_population, operator)
            except Exception as e:
                print(f'error genAreaFactor: {e}')
        else:
            # ini kalo operator sudah colo di suatu site
            # penentuan area factor berdasarkan actual revenue pada existing antena height
            try:
                status = 1
                recap['colopriming_status'] = int(1)
                # ini kalo operator sudah colo di suatu site
                area_factor = genReverseAreaFactor(tenant_revenue[operator], market_share_surrounding, arpu, colo_sectoral_population, operator)
                total_revenue, sectoral_info, grade_revenue = genReverseRevenue(total_sectoral_population, sectoral_population, market_share_surrounding, arpu, area_factor, operator)
            except Exception as e:
                print(f'error genReverseAreaFactor: {e}')
        
        ### POI
        # ini nanti jadi spatial result
        try:
            coverage_poi = gpd.overlay(poi, quadran_sectoral[['kuadran','geometry']], how='intersection', keep_geom_type=True)
            spatial_result[operator]["sectoral_poi"] = coverage_poi
        except:
            spatial_result[operator]["sectoral_poi"] = gpd.GeoDataFrame()
        
        try:
            sectoral_poi = coverage_poi.groupby('kuadran').agg(poi_count=('kuadran', 'size')).reset_index()
            sectoral_info = pd.merge(sectoral_info, sectoral_poi, on='kuadran', how='outer')
        except:
            sectoral_info['poi_count'] = 0
    ### SIGNAL LEVEL
        coverage_signal_level = 0
        try:
            coverage_signal_level = gpd.overlay(signal_level_data, quadran_sectoral[['kuadran','geometry']], how='intersection', keep_geom_type=True)
            coverage_signal_level = coverage_signal_level[(coverage_signal_level['operator'] == operator) & (coverage_signal_level['avg_rsrp'] != 0)].reset_index(drop=True)
            coverage_signal_level = np.mean(coverage_signal_level['avg_rsrp'])
            if str(coverage_signal_level) != 'nan':
                coverage_signal_level = coverage_signal_level
            else:
                coverage_signal_level = 0
        except:
            coverage_signal_level = 0
        
        ### BUILDING
        try:
            building_data['geom'] = building_data['geometry']
            building_data = building_data.set_geometry('centroid')
            building_data = building_data.to_crs('epsg:4326')
            coverage_building = gpd.overlay(building_data, quadran_sectoral[['kuadran','geometry']], how='intersection', keep_geom_type=True)
            coverage_building['centroid'] = coverage_building['geometry']
            coverage_building['geometry'] = coverage_building['geom']
            coverage_building = coverage_building.set_geometry('geometry')
            coverage_building = coverage_building.drop(['geom'], axis=1)
            coverage_building = coverage_building.to_crs('epsg:4326')
            sectoral_building = coverage_building.groupby('kuadran').agg(building_count=('kuadran', 'size'), building_area_m2=('area', 'sum')).reset_index()
            
            sectoral_info = pd.merge(sectoral_info, sectoral_building, on='kuadran', how='outer')
            
            coverage_building = coverage_building.dissolve(by='kuadran').reset_index()
            coverage_building = pd.merge(coverage_building,sectoral_building,  on='kuadran', how='outer')
            coverage_building = coverage_building[['kuadran','building_count','building_area_m2','geometry']]
            spatial_result[operator]["sectoral_building"] = coverage_building

        except:
            spatial_result[operator]["sectoral_building"] = gpd.GeoDataFrame()
            sectoral_info['building_count'] = 0

        # ### ROAD LINE
        try:
            coverage_road_line = gpd.overlay(road_line, quadran_sectoral[['kuadran','geometry']], how='intersection', keep_geom_type=True)
            coverage_road_line = coverage_road_line.to_crs('epsg:3857')
            coverage_road_line['length'] = coverage_road_line['geometry'].length
            coverage_road_line = coverage_road_line.to_crs('epsg:4326')
            sectoral_road_line = coverage_road_line.groupby('kuadran').agg(road_length_m=('length', 'sum')).reset_index()
            sectoral_info = pd.merge(sectoral_info, sectoral_road_line, on='kuadran', how='outer')
            
            coverage_road = gpd.overlay(road_data, quadran_sectoral[['kuadran','geometry']], how='intersection', keep_geom_type=True)
            coverage_road = coverage_road.to_crs('epsg:3857')
            coverage_road['area'] = coverage_road['geometry'].area
            coverage_road = coverage_road.to_crs('epsg:4326')
            coverage_road_dissolved = coverage_road.dissolve(by='kuadran').reset_index()
            sectoral_road = coverage_road.groupby('kuadran').agg(road_area_m2=('area', 'sum')).reset_index()
            sectoral_info = pd.merge(sectoral_info, sectoral_road, on='kuadran', how='outer')
            
            coverage_road_dissolved = pd.merge(coverage_road_dissolved,sectoral_road, on='kuadran', how='outer')
            coverage_road_dissolved = pd.merge(coverage_road_dissolved,sectoral_road_line[['kuadran','road_length_m']], on='kuadran', how='outer')
            coverage_road_dissolved = coverage_road_dissolved[['kuadran','road_length_m','road_area_m2','geometry']]
            spatial_result[operator]["sectoral_road"] = coverage_road_dissolved
        
        except:
            sectoral_info['road_length_m'] = 0
            sectoral_info['road_area_m2'] = 0
            spatial_result[operator]["sectoral_road"] = gpd.GeoDataFrame()
        
        # ### CLOSENESS
        try:
            coverage_closeness = gpd.overlay(closeness_data, quadran_sectoral[['kuadran','geometry']], how='intersection', keep_geom_type=True)
            closeness_score = np.mean(coverage_closeness['closeness'])
            spatial_result[operator]["sectoral_closeness"] = coverage_closeness
            sectoral_closeness = coverage_closeness.groupby('kuadran').agg(closeness_score=('closeness', 'mean')).reset_index()
            sectoral_info = pd.merge(sectoral_info, sectoral_closeness, on='kuadran', how='outer')
        except:
            closeness_score = 0
            spatial_result[operator]["sectoral_closeness"] = gpd.GeoDataFrame()
            sectoral_info['closeness_score'] = 0
        

        sectoral_info = pd.merge(sectoral_info, quadran_sectoral_area, on='kuadran', how='outer')
        sectoral_info['built_area_m2'] = sectoral_info['building_area_m2'] + sectoral_info['road_area_m2']

        if sectoral_info['built_area_m2'].sum() == 0:
            sectoral_info['built_area_ratio'] = 0
        else:
            sectoral_info['built_area_ratio'] = round((sectoral_info['built_area_m2']/sectoral_info['sectoral_area_m2']),2)
        
        site_loc_gdf= gpd.GeoDataFrame(geometry=[site_loc], crs="epsg:4326")
        site_loc_buffer = site_loc_gdf.copy()
        site_loc_buffer = site_loc_buffer.to_crs('epsg:3857')
        site_loc_buffer['geometry'] = site_loc_buffer['geometry'].buffer(10)
        site_loc_buffer = site_loc_buffer.to_crs('epsg:4326')

        line_sectoral = gpd.overlay(line_sectoral, site_loc_buffer, how='difference', keep_geom_type=True)
        line_sectoral = gpd.overlay(line_sectoral, quadran_sectoral[['kuadran','geometry']], how='difference', keep_geom_type=True)
        line_sectoral['id'] = range(1, len(line_sectoral) + 1)

        line_sectoral_buffer = line_sectoral.copy()
        line_sectoral_buffer = line_sectoral_buffer.to_crs('epsg:3857')
        line_sectoral_buffer['geometry'] = line_sectoral_buffer.buffer(0.0001)
        line_sectoral_buffer = line_sectoral_buffer.to_crs('epsg:4326')
        line_sectoral_buffer = gpd.overlay(line_sectoral_buffer, quadran_sectoral[['kuadran','geometry']], how='intersection')
        sectoral_distance = pd.merge(line_sectoral, line_sectoral_buffer[['id','kuadran']], on='id', how='left')

        sectoral_distance = sectoral_distance.to_crs('epsg:3857')
        site_loc_gdf = site_loc_gdf.to_crs('epsg:3857')
        sectoral_distance['distance'] = sectoral_distance['geometry'].distance(site_loc_gdf['geometry'][0])
        sectoral_distance = sectoral_distance.to_crs('epsg:4326')
        
        min_quandran_distance = sectoral_distance['distance'].min()
        max_quandran_distance = sectoral_distance['distance'].max()
        sectoral_distance_summary = sectoral_distance.groupby('kuadran').agg(min_sectoral_dist=('distance', 'min'), max_sectoral_dist=('distance', 'max')).reset_index()
        sectoral_info = pd.merge(sectoral_info, sectoral_distance_summary, on='kuadran', how='outer')
        
        sectoral_elevation_profile = gpd.overlay(sectoral_elevation_profile, quadran_sectoral[['kuadran','geometry']], how='intersection', keep_geom_type=True)
        
        sectoral_elevation_profile['covered'] = sectoral_elevation_profile.apply(determine_coverage, axis=1)

        spatial_result[operator]["sectoral_elevation_profile"] = sectoral_elevation_profile
        
        line_id = sectoral_elevation_profile['id'].unique()
        elevation_graphs = []
        for id in line_id:
            elevation_graph = sectoral_elevation_profile[sectoral_elevation_profile['id'] == id]['h'].to_list()
            elevation_graphs.append(str({"graph": elevation_graph}))

        sectoral_distance['elevation_graph'] = elevation_graphs
        spatial_result[operator]["sectoral_distance"] = sectoral_distance

        sectoral_info = sectoral_info.to_json(orient="records")
        sectoral_info_json = json.loads(sectoral_info)
        market_share_village_json = json.dumps([
                            {"name": name, "value": round(value * 100, 2)}
                            for name, value in market_share_competition.items()
                        ])
        market_share_surrounding_json = json.dumps([
                            {"name": name, "value": round(value * 100, 2)}
                            for name, value in market_share_surrounding.items()
                        ])
        
        grading_surrounding = gradingSurrounding(site_loc, operator, towerCoverageRadius, 0)

        try:
            overall_grade = await colopriming_grading(grading_surrounding,status,grade_revenue,coverage_signal_level,operator)
        except Exception as e:
            overall_grade = "-"


        try:
            overall_grade, potential_collocation, avg_distance, avg_distance_opt, surrounding_quality= await colopriming_grading(grading_surrounding,status,grade_revenue,coverage_signal_level,operator)
        except Exception as e:
            overall_grade, potential_collocation, avg_distance, avg_distance_opt, surrounding_quality = "-",'nok',0,0,0


        recap['overall_grade'] = overall_grade
        recap['potential_collocation'] = potential_collocation
        recap['avg_distance'] = avg_distance
        recap['avg_distance_opt'] = avg_distance_opt
        recap['surrounding_quality'] = surrounding_quality

        try:
            recap['signal_strength'] = float(coverage_signal_level)
        except:
            recap['signal_strength'] = 0

        if isSiro != True:
            recap['los_profile'] = str(losProfile)
        
        recap['gridness_ratio'] = gridness_ratio
        recap['gridness_quadran'] = gridness_quadran
        recap['total_population'] = int(total_sectoral_population)
        recap['market_share'] = market_share_surrounding[operator]

        if isSiro != True:
            recap['market_share_village'] = str(market_share_village_json)
            recap['market_share_surrounding'] = str(market_share_surrounding_json)
        else:    
            recap['market_share_village'] = json.loads(market_share_village_json)
            recap['market_share_surrounding'] = json.loads(market_share_surrounding_json)
        



        recap['area_factor'] = area_factor
        recap['arpu'] = arpu
        recap['total_revenue'] = int(total_revenue)
        recap['grade_revenue'] = grade_revenue
        recap['quadran_area_m2'] = float(total_quadran_sectoral_area)
        recap['building_count'] = int(sum(sectoral_building['building_count']))
        recap['building_area_m2'] = float(sum(coverage_building['building_area_m2']))
        recap['road_length_m'] = float(sum(coverage_road_line['length']))
        recap['road_area_m2'] = float(sum(coverage_road['area']))
        recap['closeness_score'] = float(closeness_score)
        recap['built_area_m2'] = float(recap['building_area_m2']) + float(recap['road_area_m2'])
        recap['built_area_ratio'] = round(float(recap['built_area_m2'])/float(total_quadran_sectoral_area), 2)
        recap['min_sectoral_dist'] = min_quandran_distance
        recap['max_sectoral_dist'] = max_quandran_distance
        
        recap['sectoral_info'] = sectoral_info_json

        return {"recap": recap, "spatial_result": spatial_result}
    except:
        raise

async def mainBulkOpt(opt_list, tower_height, site_loc, coloTowerCoverageRadius, towerCoverageRadius, elevationProfileCoverage, population, village_market_share, population_8000m,  opt_tenant, tenant_revenue,signal_level_data, district_ms, poi):
    opt_list = json.loads(opt_list.replace("'",'"')) 
    
    tasks = []

    async with asyncio.TaskGroup() as tg:    
        for i, operator in enumerate(opt_list):
            task = tg.create_task(bulk_process_operator(operator, tower_height, site_loc, coloTowerCoverageRadius, towerCoverageRadius, elevationProfileCoverage, population, village_market_share, population_8000m,  opt_tenant, tenant_revenue,signal_level_data,district_ms, poi))
            tasks.append(task)

    recap = [task.result()["recap"] for task in tasks]
    return recap


async def bulk_process_operator(operator, tower_height, site_loc, coloTowerCoverageRadius, towerCoverageRadius, elevationProfileCoverage, population, village_market_share, population_8000m,  opt_tenant, tenant_revenue,signal_level_data,district_ms, poi):
    recap= {}
    recap['operator'] = str(operator)

    ############################### CALCULATE POPULATION BY ACTUAL REVENUE ######################################
    colo_surrounding_sectoral = genSurroundingSectoral(site_loc, operator, coloTowerCoverageRadius, 400)
    colo_quadran_sectoral = genQuadranSectoral(site_loc, colo_surrounding_sectoral)
    colo_quadran_sectoral = gpd.overlay(colo_quadran_sectoral, elevationProfileCoverage[["geometry"]], how='intersection', keep_geom_type=True)
    colo_sectoral_population = gpd.overlay(population, colo_quadran_sectoral[['kuadran','azimuth_start','azimuth_end','geometry']], how='intersection', keep_geom_type=True)
    colo_sectoral_population = colo_sectoral_population.groupby(['kuadran','azimuth_start','azimuth_end'])['population'].sum().reset_index()
    colo_sectoral_population.columns = ['kuadran','azimuth_start','azimuth_end','total_population']
    colo_sectoral_population = colo_sectoral_population['total_population'].sum()
    #############################################################################################################
    surrounding_sectoral = genSurroundingSectoral(site_loc, operator, towerCoverageRadius, 400)
    gridness_quadran, gridness_ratio = genGridness(surrounding_sectoral, operator)
    quadran_sectoral = genQuadranSectoral(site_loc, surrounding_sectoral)
    quadran_sectoral = gpd.overlay(quadran_sectoral, elevationProfileCoverage[["geometry"]], how='intersection', keep_geom_type=True)
    quadran_sectoral = quadran_sectoral.to_crs('epsg:3857')
    quadran_sectoral['area'] = quadran_sectoral['geometry'].area
    quadran_sectoral = quadran_sectoral.to_crs('epsg:4326')
    total_quadran_sectoral_area = quadran_sectoral['area'].sum()
    quadran_sectoral_area = quadran_sectoral[['kuadran','area']]
    quadran_sectoral_area.columns = ['kuadran','sectoral_area_m2']

    poi_sectoral = gpd.overlay(poi, quadran_sectoral[['kuadran','geometry']], how='intersection', keep_geom_type=True)
    poi_sectoral = len(poi_sectoral)

    sectoral_population = gpd.overlay(population, quadran_sectoral[['kuadran','azimuth_start','azimuth_end','geometry']], how='intersection', keep_geom_type=True)
    sectoral_population = sectoral_population.groupby(['kuadran','azimuth_start','azimuth_end'])['population'].sum().reset_index()
    sectoral_population.columns = ['kuadran','azimuth_start','azimuth_end','total_population']
    total_sectoral_population = sectoral_population['total_population'].sum()
    
    try:
        market_share_competition = getVillageMarketShareOperator(village_market_share, operator)
        village_ms = market_share_competition
    except Exception as e:
        market_share_competition = district_ms
        village_ms = market_share_competition

    try:
        market_share_surrounding = await getMarketShareSurrounding(site_loc, surrounding_sectoral, operator)
    except:
        market_share_surrounding = market_share_competition

    arpu = genArpu(operator)

    if operator not in opt_tenant:
        # ini kalo operator belum colo di suatu site
        # penentuan area factor berdasarkan site di sekitarnya
        status = 0
        recap['colopriming_status'] = int(0)
        area_factor = await genAreaFactor(surrounding_sectoral, population_8000m, total_sectoral_population, operator, arpu)
        total_revenue, sectoral_info, grade_revenue, pass_max_actual_revenue= genRevenue(total_sectoral_population, sectoral_population, market_share_surrounding, arpu, area_factor, operator,surrounding_sectoral)
        if pass_max_actual_revenue == 1:
            area_factor = genReverseAreaFactor(total_revenue, market_share_surrounding, arpu, total_sectoral_population, operator)
    else:
        # ini kalo operator sudah colo di suatu site
        # penentuan area factor berdasarkan actual revenue pada existing antena height
        status = 1
        recap['colopriming_status'] = int(1)
        # ini kalo operator sudah colo di suatu site
        area_factor = genReverseAreaFactor(tenant_revenue[operator], market_share_surrounding, arpu, colo_sectoral_population, operator)
        total_revenue, sectoral_info, grade_revenue = genReverseRevenue(total_sectoral_population, sectoral_population, market_share_surrounding, arpu, area_factor, operator)
    
   ### SIGNAL LEVEL
    coverage_signal_level = 0
    try:
        coverage_signal_level = gpd.overlay(signal_level_data, quadran_sectoral[['kuadran','geometry']], how='intersection', keep_geom_type=True)
        coverage_signal_level = coverage_signal_level[(coverage_signal_level['operator'] == operator) & (coverage_signal_level['avg_rsrp'] != 0)].reset_index(drop=True)
        coverage_signal_level = np.mean(coverage_signal_level['avg_rsrp'])
        if str(coverage_signal_level) != 'nan':
            coverage_signal_level = coverage_signal_level
        else:
            coverage_signal_level = 0
    except:
        coverage_signal_level = 0

    grading_surrounding = gradingSurrounding(site_loc, operator, towerCoverageRadius, 0)

    try:
        overall_grade, potential_collocation, avg_distance, avg_distance_opt, surrounding_quality= await colopriming_grading(grading_surrounding,status,grade_revenue,coverage_signal_level,operator)
    except Exception as e:
        overall_grade, potential_collocation, avg_distance, avg_distance_opt, surrounding_quality = "-",'nok',0,0,0

    recap['operator'] = operator
    recap['overall_grade'] = overall_grade
    recap['potential_collocation'] = potential_collocation
    recap['avg_distance'] = avg_distance
    recap['avg_distance_opt'] = avg_distance_opt
    recap['surrounding_quality'] = surrounding_quality

    try:
        recap['signal_strength'] = float(coverage_signal_level)
    except:
        recap['signal_strength'] = 0
        
    recap['gridness_ratio'] = gridness_ratio
    recap['gridness_quadran'] = gridness_quadran
    recap['total_population'] = int(total_sectoral_population)
    recap['total_poi'] = int(poi_sectoral)
    recap['market_share'] = market_share_surrounding[operator]
    recap['market_share_village'] = village_ms[operator]
    recap['market_share_district'] = district_ms[operator]
    recap['area_factor'] = area_factor
    recap['arpu'] = arpu
    recap['total_revenue'] = int(total_revenue)
    recap['grade_revenue'] = grade_revenue

    return {"recap": recap}


async def bulk_process_city_data(site_loc, city_ids, elevation_profile):
    initial_data = {
        'signal_level_data': pd.DataFrame(),
        'poi':pd.DataFrame(),
    }

    road = pd.DataFrame()
    
    tasks = []
    for city_id in city_ids:
        tasks.append(asyncio.create_task(get_signal_level_city(city_id, elevation_profile[0])))
        tasks.append(asyncio.create_task(get_poi_data(site_loc, city_id, elevation_profile[0])))
    
    results = await asyncio.gather(*tasks)
    
    for i, city_id in enumerate(city_ids):
        signal_level_city = results[2 * i]
        poi = results[2 * i + 1]

        initial_data['signal_level_data'] = pd.concat([initial_data['signal_level_data'], signal_level_city], ignore_index=True)
        initial_data['poi'] = pd.concat([initial_data['poi'], poi], ignore_index=True)
    
    return initial_data.values()



async def process_operator_siro(operator, tower_height, site_loc, coloTowerCoverageRadius, towerCoverageRadius, elevationProfileCoverage, population, village_market_share, population_8000m,  opt_tenant, tenant_revenue, building_data, road_data, closeness_data, road_line, line_sectoral, sectoral_elevation_profile, poi, signal_level_data, district_ms,tp_distance):
    try:

        recap= {}
        recap['operator'] = str(operator)

        spatial_result = {operator: {}}

        ############################### CALCULATE POPULATION BY ACTUAL REVENUE ######################################
        colo_surrounding_sectoral = genSurroundingSectoralSiro(site_loc, operator, coloTowerCoverageRadius, tp_distance+20.0)
        colo_quadran_sectoral = genQuadranSectoral(site_loc, colo_surrounding_sectoral)
        colo_quadran_sectoral = gpd.overlay(colo_quadran_sectoral, elevationProfileCoverage[["geometry"]], how='intersection', keep_geom_type=True)
        colo_sectoral_population = gpd.overlay(population, colo_quadran_sectoral[['kuadran','azimuth_start','azimuth_end','geometry']], how='intersection', keep_geom_type=True)
        colo_sectoral_population = colo_sectoral_population.groupby(['kuadran','azimuth_start','azimuth_end'])['population'].sum().reset_index()
        colo_sectoral_population.columns = ['kuadran','azimuth_start','azimuth_end','total_population']
        colo_sectoral_population = colo_sectoral_population['total_population'].sum()
        #############################################################################################################
        surrounding_sectoral = genSurroundingSectoralSiro(site_loc, operator, towerCoverageRadius, tp_distance+20.0)
        gridness_quadran, gridness_ratio = genGridness(surrounding_sectoral, operator)
        quadran_sectoral = genQuadranSectoral(site_loc, surrounding_sectoral)
        quadran_sectoral = gpd.overlay(quadran_sectoral, elevationProfileCoverage[["geometry"]], how='intersection', keep_geom_type=True)
        quadran_sectoral = quadran_sectoral.to_crs('epsg:3857')
        quadran_sectoral['area'] = quadran_sectoral['geometry'].area
        quadran_sectoral = quadran_sectoral.to_crs('epsg:4326')
        total_quadran_sectoral_area = quadran_sectoral['area'].sum()
        quadran_sectoral_area = quadran_sectoral[['kuadran','area']]
        quadran_sectoral_area.columns = ['kuadran','sectoral_area_m2']

        spatial_result[operator]["quadran_sectoral"] = quadran_sectoral

        sectoral_population = gpd.overlay(population, quadran_sectoral[['kuadran','azimuth_start','azimuth_end','geometry']], how='intersection', keep_geom_type=True)
        sectoral_population = sectoral_population.groupby(['kuadran','azimuth_start','azimuth_end'])['population'].sum().reset_index()
        sectoral_population.columns = ['kuadran','azimuth_start','azimuth_end','total_population']
        total_sectoral_population = sectoral_population['total_population'].sum()

        # market_share_competition = getVillageMarketShareOperator(village_market_share, operator)
        # try:
        #     market_share_surrounding = await getMarketShareSurrounding(site_loc, surrounding_sectoral, operator)
        # except:
        #     market_share_surrounding = market_share_competition
        
        try:
            market_share_competition = getVillageMarketShareOperator(village_market_share, operator)
        except Exception as e:
            market_share_competition = district_ms

        try:
            market_share_surrounding = await getMarketShareSurrounding(site_loc, surrounding_sectoral, operator)
        except Exception as e:
            market_share_surrounding = market_share_competition
            
        arpu = genArpu(operator)

        sectoral_info = gpd.GeoDataFrame()

        if operator not in opt_tenant:
            # ini kalo operator belum colo di suatu site
            # penentuan area factor berdasarkan site di sekitarnya
            try:
                status = 0
                recap['colopriming_status'] = int(0)
                area_factor = await genAreaFactor(surrounding_sectoral, population_8000m, total_sectoral_population, operator, arpu)
                total_revenue, sectoral_info, grade_revenue, pass_max_actual_revenue = genRevenue(total_sectoral_population, sectoral_population, market_share_surrounding, arpu, area_factor, operator, surrounding_sectoral)
                if pass_max_actual_revenue == 1:
                    area_factor = genReverseAreaFactor(total_revenue, market_share_surrounding, arpu, total_sectoral_population, operator)
            except Exception as e:
                print(f'error genAreaFactor: {e}')
        else:
            # ini kalo operator sudah colo di suatu site
            # penentuan area factor berdasarkan actual revenue pada existing antena height
            try:
                status = 1
                recap['colopriming_status'] = int(1)
                # ini kalo operator sudah colo di suatu site
                area_factor = genReverseAreaFactor(tenant_revenue[operator], market_share_surrounding, arpu, colo_sectoral_population, operator)
                total_revenue, sectoral_info, grade_revenue = genReverseRevenue(total_sectoral_population, sectoral_population, market_share_surrounding, arpu, area_factor, operator)
            except Exception as e:
                print(f'error genReverseAreaFactor: {e}')
        
        ### POI
        # ini nanti jadi spatial result
        try:
            coverage_poi = gpd.overlay(poi, quadran_sectoral[['kuadran','geometry']], how='intersection', keep_geom_type=True)
            spatial_result[operator]["sectoral_poi"] = coverage_poi
        except:
            spatial_result[operator]["sectoral_poi"] = gpd.GeoDataFrame()
        
        try:
            sectoral_poi = coverage_poi.groupby('kuadran').agg(poi_count=('kuadran', 'size')).reset_index()
            sectoral_info = pd.merge(sectoral_info, sectoral_poi, on='kuadran', how='outer')
        except:
            sectoral_info['poi_count'] = 0
    ### SIGNAL LEVEL
        coverage_signal_level = 0
        try:
            coverage_signal_level = gpd.overlay(signal_level_data, quadran_sectoral[['kuadran','geometry']], how='intersection', keep_geom_type=True)
            coverage_signal_level = coverage_signal_level[(coverage_signal_level['operator'] == operator) & (coverage_signal_level['avg_rsrp'] != 0)].reset_index(drop=True)
            coverage_signal_level = np.mean(coverage_signal_level['avg_rsrp'])
            if str(coverage_signal_level) != 'nan':
                coverage_signal_level = coverage_signal_level
            else:
                coverage_signal_level = 0
        except:
            coverage_signal_level = 0
        
        ### BUILDING
        try:
            building_data['geom'] = building_data['geometry']
            building_data = building_data.set_geometry('centroid')
            building_data = building_data.to_crs('epsg:4326')
            coverage_building = gpd.overlay(building_data, quadran_sectoral[['kuadran','geometry']], how='intersection', keep_geom_type=True)
            coverage_building['centroid'] = coverage_building['geometry']
            coverage_building['geometry'] = coverage_building['geom']
            coverage_building = coverage_building.set_geometry('geometry')
            coverage_building = coverage_building.drop(['geom'], axis=1)
            coverage_building = coverage_building.to_crs('epsg:4326')
            sectoral_building = coverage_building.groupby('kuadran').agg(building_count=('kuadran', 'size'), building_area_m2=('area', 'sum')).reset_index()
            
            sectoral_info = pd.merge(sectoral_info, sectoral_building, on='kuadran', how='outer')
            
            coverage_building = coverage_building.dissolve(by='kuadran').reset_index()
            coverage_building = pd.merge(coverage_building,sectoral_building,  on='kuadran', how='outer')
            coverage_building = coverage_building[['kuadran','building_count','building_area_m2','geometry']]
            spatial_result[operator]["sectoral_building"] = coverage_building

        except:
            spatial_result[operator]["sectoral_building"] = gpd.GeoDataFrame()
            sectoral_info['building_count'] = 0

        # ### ROAD LINE
        try:
            coverage_road_line = gpd.overlay(road_line, quadran_sectoral[['kuadran','geometry']], how='intersection', keep_geom_type=True)
            coverage_road_line = coverage_road_line.to_crs('epsg:3857')
            coverage_road_line['length'] = coverage_road_line['geometry'].length
            coverage_road_line = coverage_road_line.to_crs('epsg:4326')
            sectoral_road_line = coverage_road_line.groupby('kuadran').agg(road_length_m=('length', 'sum')).reset_index()
            sectoral_info = pd.merge(sectoral_info, sectoral_road_line, on='kuadran', how='outer')
            
            coverage_road = gpd.overlay(road_data, quadran_sectoral[['kuadran','geometry']], how='intersection', keep_geom_type=True)
            coverage_road = coverage_road.to_crs('epsg:3857')
            coverage_road['area'] = coverage_road['geometry'].area
            coverage_road = coverage_road.to_crs('epsg:4326')
            coverage_road_dissolved = coverage_road.dissolve(by='kuadran').reset_index()
            sectoral_road = coverage_road.groupby('kuadran').agg(road_area_m2=('area', 'sum')).reset_index()
            sectoral_info = pd.merge(sectoral_info, sectoral_road, on='kuadran', how='outer')
            
            coverage_road_dissolved = pd.merge(coverage_road_dissolved,sectoral_road, on='kuadran', how='outer')
            coverage_road_dissolved = pd.merge(coverage_road_dissolved,sectoral_road_line[['kuadran','road_length_m']], on='kuadran', how='outer')
            coverage_road_dissolved = coverage_road_dissolved[['kuadran','road_length_m','road_area_m2','geometry']]
            spatial_result[operator]["sectoral_road"] = coverage_road_dissolved
        
        except:
            sectoral_info['road_length_m'] = 0
            sectoral_info['road_area_m2'] = 0
            spatial_result[operator]["sectoral_road"] = gpd.GeoDataFrame()
        
        # ### CLOSENESS
        try:
            coverage_closeness = gpd.overlay(closeness_data, quadran_sectoral[['kuadran','geometry']], how='intersection', keep_geom_type=True)
            closeness_score = np.mean(coverage_closeness['closeness'])
            spatial_result[operator]["sectoral_closeness"] = coverage_closeness
            sectoral_closeness = coverage_closeness.groupby('kuadran').agg(closeness_score=('closeness', 'mean')).reset_index()
            sectoral_info = pd.merge(sectoral_info, sectoral_closeness, on='kuadran', how='outer')
        except:
            closeness_score = 0
            spatial_result[operator]["sectoral_closeness"] = gpd.GeoDataFrame()
            sectoral_info['closeness_score'] = 0
        

        sectoral_info = pd.merge(sectoral_info, quadran_sectoral_area, on='kuadran', how='outer')
        sectoral_info['built_area_m2'] = sectoral_info['building_area_m2'] + sectoral_info['road_area_m2']

        if sectoral_info['built_area_m2'].sum() == 0:
            sectoral_info['built_area_ratio'] = 0
        else:
            sectoral_info['built_area_ratio'] = round((sectoral_info['built_area_m2']/sectoral_info['sectoral_area_m2']),2)
        
        site_loc_gdf= gpd.GeoDataFrame(geometry=[site_loc], crs="epsg:4326")
        site_loc_buffer = site_loc_gdf.copy()
        site_loc_buffer = site_loc_buffer.to_crs('epsg:3857')
        site_loc_buffer['geometry'] = site_loc_buffer['geometry'].buffer(10)
        site_loc_buffer = site_loc_buffer.to_crs('epsg:4326')

        line_sectoral = gpd.overlay(line_sectoral, site_loc_buffer, how='difference', keep_geom_type=True)
        line_sectoral = gpd.overlay(line_sectoral, quadran_sectoral[['kuadran','geometry']], how='difference', keep_geom_type=True)
        line_sectoral['id'] = range(1, len(line_sectoral) + 1)

        line_sectoral_buffer = line_sectoral.copy()
        line_sectoral_buffer = line_sectoral_buffer.to_crs('epsg:3857')
        line_sectoral_buffer['geometry'] = line_sectoral_buffer.buffer(0.0001)
        line_sectoral_buffer = line_sectoral_buffer.to_crs('epsg:4326')
        line_sectoral_buffer = gpd.overlay(line_sectoral_buffer, quadran_sectoral[['kuadran','geometry']], how='intersection')
        sectoral_distance = pd.merge(line_sectoral, line_sectoral_buffer[['id','kuadran']], on='id', how='left')

        sectoral_distance = sectoral_distance.to_crs('epsg:3857')
        site_loc_gdf = site_loc_gdf.to_crs('epsg:3857')
        sectoral_distance['distance'] = sectoral_distance['geometry'].distance(site_loc_gdf['geometry'][0])
        sectoral_distance = sectoral_distance.to_crs('epsg:4326')
        
        min_quandran_distance = sectoral_distance['distance'].min()
        max_quandran_distance = sectoral_distance['distance'].max()
        sectoral_distance_summary = sectoral_distance.groupby('kuadran').agg(min_sectoral_dist=('distance', 'min'), max_sectoral_dist=('distance', 'max')).reset_index()
        sectoral_info = pd.merge(sectoral_info, sectoral_distance_summary, on='kuadran', how='outer')
        
        sectoral_elevation_profile = gpd.overlay(sectoral_elevation_profile, quadran_sectoral[['kuadran','geometry']], how='intersection', keep_geom_type=True)
        
        sectoral_elevation_profile['covered'] = sectoral_elevation_profile.apply(determine_coverage, axis=1)

        spatial_result[operator]["sectoral_elevation_profile"] = sectoral_elevation_profile
        
        line_id = sectoral_elevation_profile['id'].unique()
        elevation_graphs = []
        for id in line_id:
            elevation_graph = sectoral_elevation_profile[sectoral_elevation_profile['id'] == id]['h'].to_list()
            elevation_graphs.append(str({"graph": elevation_graph}))

        sectoral_distance['elevation_graph'] = elevation_graphs
        spatial_result[operator]["sectoral_distance"] = sectoral_distance

        sectoral_info = sectoral_info.to_json(orient="records")
        sectoral_info_json = json.loads(sectoral_info)
        market_share_village_json = json.dumps([
                            {"name": name, "value": round(value * 100, 2)}
                            for name, value in market_share_competition.items()
                        ])
        market_share_surrounding_json = json.dumps([
                            {"name": name, "value": round(value * 100, 2)}
                            for name, value in market_share_surrounding.items()
                        ])
        
        grading_surrounding = gradingSurrounding(site_loc, operator, towerCoverageRadius, 0)

        try:
            overall_grade = await colopriming_grading(grading_surrounding,status,grade_revenue,coverage_signal_level,operator)
        except Exception as e:
            overall_grade = "-"


        try:
            overall_grade, potential_collocation, avg_distance, avg_distance_opt, surrounding_quality= await colopriming_grading(grading_surrounding,status,grade_revenue,coverage_signal_level,operator)
        except Exception as e:
            overall_grade, potential_collocation, avg_distance, avg_distance_opt, surrounding_quality = "-",'nok',0,0,0


        recap['overall_grade'] = overall_grade
        recap['potential_collocation'] = potential_collocation
        recap['avg_distance'] = avg_distance
        recap['avg_distance_opt'] = avg_distance_opt
        recap['surrounding_quality'] = surrounding_quality

        try:
            recap['signal_strength'] = float(coverage_signal_level)
        except:
            recap['signal_strength'] = 0

        recap['gridness_ratio'] = gridness_ratio
        recap['gridness_quadran'] = gridness_quadran
        recap['total_population'] = int(total_sectoral_population)
        recap['market_share'] = market_share_surrounding[operator]
        recap['market_share_village'] = json.loads(market_share_village_json)
        recap['market_share_surrounding'] = json.loads(market_share_surrounding_json)
        recap['area_factor'] = area_factor
        recap['arpu'] = arpu
        recap['total_revenue'] = int(total_revenue)
        recap['grade_revenue'] = grade_revenue
        recap['quadran_area_m2'] = float(total_quadran_sectoral_area)
        recap['building_count'] = int(sum(sectoral_building['building_count']))
        recap['building_area_m2'] = float(sum(coverage_building['building_area_m2']))
        recap['road_length_m'] = float(sum(coverage_road_line['length']))
        recap['road_area_m2'] = float(sum(coverage_road['area']))
        recap['closeness_score'] = float(closeness_score)
        recap['built_area_m2'] = float(recap['building_area_m2']) + float(recap['road_area_m2'])
        recap['built_area_ratio'] = round(float(recap['built_area_m2'])/float(total_quadran_sectoral_area), 2)
        recap['min_sectoral_dist'] = min_quandran_distance
        recap['max_sectoral_dist'] = max_quandran_distance
        
        recap['sectoral_info'] = sectoral_info_json

        return {"recap": recap, "spatial_result": spatial_result}
    except:
        raise


async def mainOptSiro(colopriming_site, site_loc, coloTowerCoverageRadius, towerCoverageRadius, elevationProfileCoverage, population, village_market_share, population_8000m,  opt_tenant, tenant_revenue, building_data, road_data, closeness_data, road_line, line_sectoral, sectoral_elevation_profile, poi, signal_level_data,district_ms):
    opt_list = colopriming_site['operator']
    tower_height = colopriming_site['tower_height']
    tp_distance = float(colopriming_site['tp_distance'])

    tasks = []

    async with asyncio.TaskGroup() as tg:    
        for i, operator in enumerate(opt_list):
            task = tg.create_task(process_operator_siro(operator, tower_height, site_loc, coloTowerCoverageRadius, towerCoverageRadius, elevationProfileCoverage, population, village_market_share, population_8000m,  opt_tenant, tenant_revenue, building_data, road_data, closeness_data, road_line, line_sectoral, sectoral_elevation_profile, poi, signal_level_data,district_ms,tp_distance))
            tasks.append(task)

    recap = [task.result()["recap"] for task in tasks]
    spatial = [task.result()["spatial_result"] for task in tasks]

    return recap, spatial