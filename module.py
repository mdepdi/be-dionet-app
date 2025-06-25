import pandas as pd
import geopandas as gpd
from sqlalchemy import text
from database import engine
import json
from shapely import Point
import numpy as np

from fastapi import HTTPException

from pydantic import ValidationError
from modules.colopriming_process import *
from modules.database_actions import *
from database import *
from datetime import datetime

import traceback
import time

from celery.utils.log import get_task_logger
celery_log = get_task_logger(__name__)

def capitalize_first_letter(text):
    return " ".join(word.capitalize() for word in text.split("_"))

async def colopriming_analysis(colopriming_site, record_id, task_id, task, running_type, isSiro=False):

    status = "STARTED"
    print(f"==>> status: {status}")

    if running_type == 'background':
        await delete_old_project(colopriming_site["site_id"], colopriming_site["antena_height"], record_id, colopriming_site["site_type"])
        update = {'status': status}
        await update_record(pColopriming,record_id, update)
    elif running_type == 'direct':
        colopriming_site = colopriming_site.__dict__
        await delete_old_project(colopriming_site["site_id"], colopriming_site["antena_height"], record_id, colopriming_site["site_type"])
        update = {'task_id': str(f'{colopriming_site["site_id"]}_{record_id}'), 'status': status, 'created_at': datetime.now()}

        await update_record(pColopriming,record_id, update)
    else:
        colopriming_site = colopriming_site.__dict__

    project = {
        "project_id": record_id,
    }

    try:
        if running_type == 'background':
            update = {'status': status}
            await update_record(pColopriming,record_id, update)

            celery_log.info('Project were started', project)
            celery_log.info(f"==>> task.status: {status}")

        print(f"==>> status: {status}")

        site_loc = Point(colopriming_site["longitude"], colopriming_site["latitude"])

        comid_city_2500m = await getComidCity2500m(site_loc)
        print(f"==>> comid_city_2500m: {comid_city_2500m}")


        city_ids = comid_city_2500m
        city_ids_str = "'" + "','".join(str(num) for num in city_ids) + "'"

        if len(comid_city_2500m)>1:
            comid_city_2500m = '_'.join(map(str, comid_city_2500m))
        else:
            comid_city_2500m = str(comid_city_2500m[0])

        comid_city_8000m = await genComidCity8000m(site_loc)

        if colopriming_site["site_type"] == "existing":
            opt_tenant, tenant_revenue, colo_antena_height = await getColoStatus(colopriming_site["site_id"])
        else:
            opt_tenant, tenant_revenue, colo_antena_height = await getExistingSite(site_loc)

        if colo_antena_height == 0:
            colopriming_site["site_type"] = "build-to-suit"

        maxTowerCoverageRadius = CoverageRadius(colopriming_site['tower_height'])
        maxTowerCoverageBufferArea = drawBuffer(colopriming_site["longitude"], colopriming_site["latitude"], maxTowerCoverageRadius)

        towerCoverageRadius = CoverageRadius(colopriming_site["antena_height"])
        towerCoverageBufferArea = drawBuffer(colopriming_site["longitude"], colopriming_site["latitude"], towerCoverageRadius)

        coloTowerCoverageRadius = CoverageRadius(colo_antena_height)
        coloCoverageBufferArea = drawBuffer(colopriming_site["longitude"], colopriming_site["latitude"], towerCoverageRadius)

        elevationProfile = await sectoralElevationProfile(colopriming_site["longitude"], colopriming_site["latitude"], colopriming_site["tower_height"], 2, 180, 0.5, maxTowerCoverageRadius, maxTowerCoverageBufferArea)

        elevationProfileCoverage, line_sectoral, sectoral_elevation_profile, elevationProfileinfo = elevationProfile
        coverageGeometry = elevationProfileCoverage['geometry'].iloc[0]

        # 1. Village
        village, household_consumption, pdrb, comid_district = await getVillageHcPdrb(site_loc)
        # print(f"==>> village: {village}")
        # 2. District
        district_competition, district_competition_json, operator_list, district_geometry= await districtCompetition(site_loc, comid_district)
        # 3. District Market Share
        village_market_share = await getVillageMarketShare(site_loc, village)
        # 4. Village Market Share
        district_ms = await districtMarketShare(district_geometry)

        # Convert the dictionary to the desired list format
        district_ms_json = json.dumps([
            {"name": name, "value": round(value * 100, 2)}
            for name, value in district_ms.items()
        ])


        # 5. Surrounding Competition
        surrounding_competition = await surroundingCompetition(site_loc)


        # 6. Relative Weight Index
        rwi = await getRwi(city_ids_str, coverageGeometry)
        # 7. Internet Speed Test
        speed_test = await getInternetSpeedTest(city_ids_str, coverageGeometry)
        # 8. Nearest Colopriming Site
        nearest_colopriming_site = await getNearestColoprimingSite(site_loc)

        #9. Get Population
        population, population_8000m, total_coverage_population = await process_population_data(site_loc, comid_city_8000m, elevationProfile)

        print('to_this_point')

        #11. Get Signal level, #12. Get Building, #13. Get Road Buffer, #14. Get Closeness
        signal_level,signal_level_data, building, road_line, road_buffer, closeness, poi= await process_city_data(site_loc, city_ids, elevationProfile)
        signal_level['name'] = 'operator'
        signal_level_json = json.dumps([signal_level])


        poi_count = poi.groupby(['category','main_color']).size().reset_index(name="count")
        poi_count = poi_count.sort_values(by='count', ascending=False).reset_index(drop=True)

        poi_count['category'] = poi_count['category'].apply(capitalize_first_letter)

        poi_count_json = json.dumps(poi_count.to_dict('records'))
        ############# CALCULTATE OPERATOR ################
        optResult, spatialResult = await mainOpt(colopriming_site, site_loc, coloTowerCoverageRadius, towerCoverageRadius, elevationProfileCoverage, population, village_market_share, population_8000m, opt_tenant, tenant_revenue, building, road_buffer, closeness, road_line, line_sectoral, sectoral_elevation_profile, poi, signal_level_data,district_ms, isSiro)

        if running_type == 'dev':
            # print(f"==>> optResult: {optResult}")
            return {"message":"success", "status": status, "project": project}


        revenueResult = [{key: d[key] for key in ['operator', 'total_revenue'] if key in d}
                        for d in optResult if 'operator' in d and 'total_revenue' in d]
        siteMarketShare = getSiteMarketShare(opt_tenant, revenueResult)

        ############ DATABASE STORING ####################
        coverage_profile = {
        "bottom_tower_elevation_mdpl":round(float(elevationProfileinfo["topTowerElevation"]-colopriming_site["tower_height"]),1),
        "top_tower_elevation_mdpl":round(float(elevationProfileinfo["topTowerElevation"]),1),
        "antena_elevation_mdpl":round(float((elevationProfileinfo["topTowerElevation"]-colopriming_site["tower_height"])+colopriming_site["antena_height"]),1),
        "min_coverage_elevation_mdpl":round(float(elevationProfileinfo["minCoverageElevation"]),1),
        "max_coverage_elevation_mdpl":round(float(elevationProfileinfo["maxCoverageElevation"]),1),
        "topografi_coverage_ratio":round(float(elevationProfileinfo["coveragePerBufferArea"]),1),
        "coverage_area_m2": round(float(elevationProfileinfo["coverageArea"]),1),
        "coverage_population":int(total_coverage_population)
        }

        economy_profile = {
            "household_consumption":round(household_consumption),
            "monthly_pdrb":round(pdrb['monthly'],1),
            "annualy_pdrb":round(pdrb['annually'],1),
            "min_rwi": round(float(rwi['min']),1),
            "max_rwi": round(float(rwi['max']),1),
        }

        competition_profile = {
            "district_competition":district_competition_json,
            "surrounding_competition": surrounding_competition,
        }

        colopriming_status = {
            "tenant":opt_tenant,
        }

        dict_update = {
            "tower_height":float(colopriming_site["tower_height"]),
            "coverage_profile":str(coverage_profile),
            "economy_profile":str(economy_profile),
            "netspeed_profile":str(speed_test),
            "signal_profile":str(signal_level_json),
            "competition_profile":str(competition_profile),
            "market_share_district":str(district_ms_json),
            "nearest_colopriming":str(nearest_colopriming_site),
            "colopriming_status":str(colopriming_status),
            "poi_profile":str(poi_count_json),
        }

        if running_type != "dev":
            await update_record(pColopriming,record_id, dict_update)

        for opt_detail in optResult:

            detail_project = project.copy()
            sectoral_project = project.copy()
            for key in opt_detail.keys():
                if key != "sectoral_info":
                    detail_project[key] = opt_detail[key]
                    detail_project['site_id'] = colopriming_site["site_id"]
                    detail_project['tower_height'] = colopriming_site["tower_height"]
                    detail_project['antena_height'] = colopriming_site["antena_height"]

                    if isSiro != True:
                        detail_project['market_share_district'] = str(district_ms_json)
                    else:
                        detail_project['market_share_district'] = json.loads(district_ms_json)

                    site_ms_json = json.dumps([
                        {"name": name, "value": round(value * 100, 2)}
                        for name, value in siteMarketShare[opt_detail["operator"]].items()
                    ])

                    detail_project['market_share_site'] = str(site_ms_json)
                else:
                    for sectoral_detail in opt_detail["sectoral_info"]:
                        for key in sectoral_detail.keys():
                            sectoral_project['operator'] = opt_detail['operator']
                            sectoral_project['site_id'] = colopriming_site["site_id"]
                            sectoral_project['tower_height'] = colopriming_site["tower_height"]
                            sectoral_project['antena_height'] = colopriming_site["antena_height"]
                            sectoral_project[key] = sectoral_detail[key]

                        await insert_record(pdsColopriming, sectoral_project)
            await insert_record(pdColopriming, detail_project)

        ##### STROING SPATIAL RESULT ######
        try:
            operator_mapping = {
            "TSEL": "istsel",
            "XL": "isxl",
            "IOH": "isioh",
            "SF": "issf"
            }
            for sr in spatialResult:
                for opt in sr.keys():
                    for layer in sr[opt].keys():
                        if layer == "sectoral_poi":
                            if opt in operator_mapping:
                                sr[opt][layer][operator_mapping[opt]] = 1
                                poi = pd.merge(poi, sr[opt][layer][['id', operator_mapping[opt]]], on='id', how='outer')

            # Ensure the columns exist after merging
            for col in ['istsel', 'isxl', 'isioh', 'issf']:
                if col not in poi.columns:
                    poi[col] = 0

            # Replace NaN values with 0 in the integer columns
            poi['istsel'].fillna(0, inplace=True)
            poi['isxl'].fillna(0, inplace=True)
            poi['isioh'].fillna(0, inplace=True)
            poi['issf'].fillna(0, inplace=True)

            # Convert these columns to integer type to avoid any further issues
            poi['istsel'] = poi['istsel'].astype(int)
            poi['isxl'] = poi['isxl'].astype(int)
            poi['isioh'] = poi['isioh'].astype(int)
            poi['issf'] = poi['issf'].astype(int)


            poi = poi[(poi['istsel'] == 1) | (poi['isxl'] == 1) | (poi['isioh'] == 1) | (poi['issf'] == 1)].reset_index(drop=True)

            for idx , i in poi.iterrows():
                geometry_wkt = i['geometry'].wkt if i['geometry'] else None
                try:
                    data = {
                        "project_id" : record_id,
                        "site_id" : colopriming_site["site_id"],
                        "tower_height" : colopriming_site["tower_height"],
                        "antena_height" : colopriming_site["antena_height"],
                        "kuadran" : 0,
                        "poi_id" : i['id'],
                        "updatetime" : i['updatetime'],
                        "confidence" : i['confidence'],
                        "category" : i['category'],
                        "sub_category" : i['sub_category'],
                        "name" : i['name'],
                        "website" : i['website'],
                        "phone" : i['phone'],
                        "source" : i['source'],
                        "color" : i['color'],
                        "main_color" : i['main_color'],
                        "istsel": i['istsel'],
                        "isxl": i['isxl'],
                        "isioh": i['isioh'],
                        "issf": i['issf'],
                        "distance": i['distance'],
                        "geometry" : geometry_wkt
                    }
                    await insert_record(spSectoralPoi,data)
                except KeyError as e:
                    print(f"KeyError: {e} not found in dictionary. Skipping entry.")
                except (ValueError, TypeError) as e:
                    print(f"Error converting value akuifer data: {e}. Skipping entry.")

        except Exception as e:
            status = "FAILURE"
            tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
            if running_type == 'background':
                update = {'status': status}
                await update_record(pColopriming,record_id, update)
                celery_log.info(f"==>> task.status: {status}")
                celery_log.info(f"==>> CELERY ERROR: {tb_str}")
            elif running_type == 'direct':
                update = {'status': status}
                await update_record(pColopriming,record_id, update)
                print(f"==>> status: {status}")
            else:
                print(f"==>> status: {status}")

            raise HTTPException(status_code=500, detail=f'ERROR: {e}\n{tb_str}')

        try:
            geometry_wkt = coverageGeometry.wkt if coverageGeometry else None
            data = {
                "project_id" : record_id,
                "site_id" : colopriming_site["site_id"],
                "tower_height" : colopriming_site["tower_height"],
                "antena_height" : colopriming_site["antena_height"],
                "area" : round(float(elevationProfileinfo["coverageArea"]),1),
                "geometry" : geometry_wkt
            }
            await insert_record(spCoverageRadius,data)
        except KeyError as e:
            print(f"KeyError: {e} not found in dictionary. Skipping entry.")
        except (ValueError, TypeError) as e:
            print(f"Error converting value akuifer data: {e}. Skipping entry.")


        for sr in spatialResult:
            for opt in sr.keys():
                for layer in sr[opt].keys():
                    if layer == "quadran_sectoral":
                        for idx , i in sr[opt][layer].iterrows():
                            geometry_wkt = i['geometry'].wkt if i['geometry'] else None
                            try:
                                data = {
                                    "project_id" : record_id,
                                    "site_id" : colopriming_site["site_id"],
                                    "tower_height" : colopriming_site["tower_height"],
                                    "antena_height" : colopriming_site["antena_height"],
                                    "operator" : opt,
                                    "kuadran" : i['kuadran'],
                                    "distance" : i['distance'],
                                    "area" : i['area'],
                                    "geometry" : geometry_wkt
                                }
                                await insert_record(spQuadranSec,data)
                            except KeyError as e:
                                print(f"KeyError: {e} not found in dictionary. Skipping entry.")
                            except (ValueError, TypeError) as e:
                                print(f"Error converting value akuifer data: {e}. Skipping entry.")


                    if layer == "sectoral_building":
                        for idx , i in sr[opt][layer].iterrows():
                            geometry_wkt = i['geometry'].wkt if i['geometry'] else None
                            try:
                                data = {
                                    "project_id" : record_id,
                                    "site_id" : colopriming_site["site_id"],
                                    "tower_height" : colopriming_site["tower_height"],
                                    "antena_height" : colopriming_site["antena_height"],
                                    "operator" : opt,
                                    "kuadran" : i['kuadran'],
                                    "count" : i['building_count'],
                                    "area" : i['building_area_m2'],
                                    "geometry" : geometry_wkt
                                }
                                await insert_record(spBuildingSec,data)
                            except KeyError as e:
                                print(f"KeyError: {e} not found in dictionary. Skipping entry.")
                            except (ValueError, TypeError) as e:
                                print(f"Error converting value akuifer data: {e}. Skipping entry.")

                    if layer == "sectoral_road":
                        for idx , i in sr[opt][layer].iterrows():
                            geometry_wkt = i['geometry'].wkt if i['geometry'] else None
                            try:
                                data = {
                                    "project_id" : record_id,
                                    "site_id" : colopriming_site["site_id"],
                                    "tower_height" : colopriming_site["tower_height"],
                                    "antena_height" : colopriming_site["antena_height"],
                                    "operator" : opt,
                                    "kuadran" : i['kuadran'],
                                    "length" : i['road_length_m'],
                                    "area" : i['road_area_m2'],
                                    "geometry" : geometry_wkt
                                }
                                await insert_record(spRoadSec,data)
                            except KeyError as e:
                                print(f"KeyError: {e} not found in dictionary. Skipping entry.")
                            except (ValueError, TypeError) as e:
                                print(f"Error converting value akuifer data: {e}. Skipping entry.")

                    if layer == "sectoral_closeness":
                        for idx , i in sr[opt][layer].iterrows():
                            geometry_wkt = i['geometry'].wkt if i['geometry'] else None
                            try:
                                data = {
                                    "project_id" : record_id,
                                    "site_id" : colopriming_site["site_id"],
                                    "tower_height" : colopriming_site["tower_height"],
                                    "antena_height" : colopriming_site["antena_height"],
                                    "operator" : opt,
                                    "kuadran" : i['kuadran'],
                                    "closeness" : i['closeness'],
                                    "geometry" : geometry_wkt
                                }
                                await insert_record(spRoadCloseness,data)
                            except KeyError as e:
                                print(f"KeyError: {e} not found in dictionary. Skipping entry.")
                            except (ValueError, TypeError) as e:
                                print(f"Error converting value akuifer data: {e}. Skipping entry.")

                    if layer == "sectoral_elevation_profile":
                        for idx , i in sr[opt][layer].iterrows():
                            geometry_wkt = i['geometry'].wkt if i['geometry'] else None
                            try:
                                data = {
                                    "project_id" : record_id,
                                    "site_id" : colopriming_site["site_id"],
                                    "tower_height" : colopriming_site["tower_height"],
                                    "antena_height" : colopriming_site["antena_height"],
                                    "operator" : opt,
                                    "kuadran" : i['kuadran'],
                                    "line_id": i['id'],
                                    "distance" : i['distance_to_center'],
                                    "max_distance" : i['max_distance'],
                                    "ground_height" : i['h'],
                                    "signal_height" : i['tdh'],
                                    "covered" : i['covered'],
                                    "geometry" : geometry_wkt
                                }
                                await insert_record(spSectoralElevation,data)
                            except KeyError as e:
                                print(f"KeyError: {e} not found in dictionary. Skipping entry.")
                            except (ValueError, TypeError) as e:
                                print(f"Error converting value akuifer data: {e}. Skipping entry.")

                    if layer == "sectoral_distance":
                        for idx , i in sr[opt][layer].iterrows():
                            geometry_wkt = i['geometry'].wkt if i['geometry'] else None
                            try:
                                data = {
                                    "project_id" : record_id,
                                    "site_id" : colopriming_site["site_id"],
                                    "tower_height" : colopriming_site["tower_height"],
                                    "antena_height" : colopriming_site["antena_height"],
                                    "operator" : opt,
                                    "kuadran" : i['kuadran'],
                                    "line_id": i['id'],
                                    "distance" : i['distance'],
                                    "graph":i['elevation_graph'],
                                    "geometry" : geometry_wkt
                                }
                                await insert_record(spSectoralDistance,data)
                            except KeyError as e:
                                print(f"KeyError: {e} not found in dictionary. Skipping entry.")
                            except (ValueError, TypeError) as e:
                                print(f"Error converting value akuifer data: {e}. Skipping entry.")
        status = "SUCCESS"

        return {"message":"success", "status": status, "project": project}

        # return {"message":"success", "status": status}

    except ValidationError as e:
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        status = "FAILURE"
        if running_type == 'background':
            update = {'status': status, 'error_message': tb_str}
            await update_record(pColopriming,record_id, update)

            celery_log.info(f"==>> task.status: {status}")
            celery_log.info(f"==>> CELERY ERROR: {tb_str}")
        elif running_type == 'direct':
            update = {'status': status, 'error_message': tb_str}
            await update_record(pColopriming,record_id, update)
            print(f"==>> status: {status}")
        else:
            print(f"==>> status: {status}")


        raise HTTPException(status_code=500, detail=f'VALIDATION ERROR: {e}\n{tb_str}')

    except Exception as e:
        status = "FAILURE"
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        if running_type == 'background':
            update = {'status': status, 'error_message': tb_str}
            await update_record(pColopriming,record_id, update)
            celery_log.info(f"==>> task.status: {status}")
            celery_log.info(f"==>> CELERY ERROR: {tb_str}")
        elif running_type == 'direct':
            update = {'status': status, 'error_message': tb_str}
            await update_record(pColopriming,record_id, update)
            print(f"==>> status: {status}")
        else:
            print(f"==>> status: {status}")

        raise HTTPException(status_code=500, detail=f'ERROR: {e}\n{tb_str}')

    finally:
        if running_type == 'background':
            update = {'status': status, 'finished_at': datetime.now()}
            await update_record(pColopriming,record_id, update)
            celery_log.info(f"==>> status: {status}")
        elif running_type == 'direct':
            update = {'status': status, 'finished_at': datetime.now()}
            await update_record(pColopriming,record_id, update)
            print(f"==>> status: {status}")
        else:
            print(f"==>> status: {status}")



async def bulk_colopriming_analysis(colopriming_site):
    # print(colopriming_site['id'])
    try:
        site_loc = Point(colopriming_site["longitude"], colopriming_site["latitude"])

        comid_city_2500m = await getComidCity2500m(site_loc)

        if len(comid_city_2500m)>1:
            comid_city_2500m = comid_city_2500m
        else:
            comid_city_2500m = comid_city_2500m

        city_ids = comid_city_2500m
        comid_city_8000m = await genComidCity8000m(site_loc)

        if colopriming_site["site_type"] == "existing":
            opt_tenant, tenant_revenue, colo_antena_height = await getColoStatus(colopriming_site["site_id"])
        else:
            opt_tenant, tenant_revenue, colo_antena_height = await getExistingSite(site_loc)


        if colo_antena_height == 0:
            colopriming_site["site_type"] = "build-to-suit"
            colo_antena_height = colopriming_site["antena_height"]

        maxTowerCoverageRadius = CoverageRadius(colopriming_site['tower_height'])
        maxTowerCoverageBufferArea = drawBuffer(colopriming_site["longitude"], colopriming_site["latitude"], maxTowerCoverageRadius)

        towerCoverageRadius = CoverageRadius(colopriming_site["antena_height"])
        towerCoverageBufferArea = drawBuffer(colopriming_site["longitude"], colopriming_site["latitude"], towerCoverageRadius)

        coloTowerCoverageRadius = CoverageRadius(colo_antena_height)
        coloCoverageBufferArea = drawBuffer(colopriming_site["longitude"], colopriming_site["latitude"], towerCoverageRadius)

        elevationProfile = await sectoralElevationProfile(colopriming_site["longitude"], colopriming_site["latitude"], colopriming_site["tower_height"], 2, 180, 0.5, maxTowerCoverageRadius, maxTowerCoverageBufferArea)

        elevationProfileCoverage, line_sectoral, sectoral_elevation_profile, elevationProfileinfo = elevationProfile
        coverageGeometry = elevationProfileCoverage['geometry'].iloc[0]

        # 1. Village
        village, household_consumption, pdrb, comid_district = await getVillageHcPdrb(site_loc)
        # print(f"==>> village: {village}")
        # 2. District
        district_competition, district_competition_json, operator_list, district_geometry= await districtCompetition(site_loc, comid_district)
        # 3. District Market Share
        village_market_share = await getVillageMarketShare(site_loc, village)
        # 4. Village Market Share
        district_ms = await districtMarketShare(district_geometry)
        # # Convert the dictionary to the desired list format

        # #9. Get Population
        population, population_8000m, total_coverage_population = await process_population_data(site_loc, comid_city_8000m, elevationProfile)
           #11. Get Signal level, #12. Get Building, #13. Get Road Buffer, #14. Get Closeness
        signal_level_data, poi= await bulk_process_city_data(site_loc, city_ids, elevationProfile)

        # # ############CALCULTATE OPERATOR####################

        optResult = await mainBulkOpt(colopriming_site["operator"],colopriming_site["tower_height"], site_loc, coloTowerCoverageRadius, towerCoverageRadius, elevationProfileCoverage, population, village_market_share, population_8000m, opt_tenant, tenant_revenue,signal_level_data,district_ms, poi)
        # status = "SUCCESS"

        return {"message":"success", "result":optResult}

    except ValidationError as e:
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        raise HTTPException(status_code=500, detail=f'VALIDATION ERROR: {e}\n{tb_str}')

    except Exception as e:
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        raise HTTPException(status_code=500, detail=f'ERROR: {e}\n{tb_str}')



async def colopriming_siro_analysis(colopriming_site, record_id,  task_id, task, running_type, isSiro=True):
# async def colopriming_siro_analysis(colopriming_site):

    status = "STARTED"

    if running_type == 'background':
        await delete_old_siro_project(colopriming_site["site_id"],colopriming_site["operator"][0], record_id)
        update = {'status': status}
        await update_record(pSiro,record_id, update)
    elif running_type == 'direct':
        colopriming_site = colopriming_site.__dict__
        await delete_old_siro_project(colopriming_site["site_id"], colopriming_site["operator"][0], record_id)
        update = {'task_id': str(f'{colopriming_site["site_id"]}_{record_id}'), 'status': status, 'created_at': datetime.now()}

        await update_record(pSiro,record_id, update)
    else:
        colopriming_site = colopriming_site.__dict__

    project = {
        "project_id": record_id,
    }

    try:
        if running_type == 'background':
            update = {'status': status}
            await update_record(pSiro,record_id, update)

            celery_log.info('Project were started', project)
            celery_log.info(f"==>> task.status: {status}")

        siro_status = await getSiroStatus(colopriming_site['site_id'],colopriming_site['operator'][0])

        site_loc = Point(colopriming_site["longitude"], colopriming_site["latitude"])

        comid_city_2500m = await getComidCity2500m(site_loc)
        city_ids = comid_city_2500m
        city_ids_str = "'" + "','".join(str(num) for num in city_ids) + "'"

        if len(comid_city_2500m)>1:
            comid_city_2500m = '_'.join(map(str, comid_city_2500m))
        else:
            comid_city_2500m = str(comid_city_2500m[0])

        comid_city_8000m = await genComidCity8000m(site_loc)

        opt_tenant, tenant_revenue, colo_antena_height = await getColoStatus(colopriming_site["site_id"])

        maxTowerCoverageRadius = CoverageRadius(colopriming_site['tower_height'])
        maxTowerCoverageBufferArea = drawBuffer(colopriming_site["longitude"], colopriming_site["latitude"], maxTowerCoverageRadius)

        towerCoverageRadius = CoverageRadius(colopriming_site["antena_height"])

        coloTowerCoverageRadius = CoverageRadius(colo_antena_height)

        elevationProfile = await sectoralElevationProfile(colopriming_site["longitude"], colopriming_site["latitude"], colopriming_site["tower_height"], 2, 180, 0.5, maxTowerCoverageRadius, maxTowerCoverageBufferArea)

        elevationProfileCoverage, line_sectoral, sectoral_elevation_profile, elevationProfileinfo = elevationProfile

        # 1. Village
        village, household_consumption, pdrb, comid_district = await getVillageHcPdrb(site_loc)
        # # print(f"==>> village: {village}")
        # 2. District
        district_competition, district_competition_json, operator_list, district_geometry= await districtCompetition(site_loc, comid_district)
        # 3. District Market Share
        village_market_share = await getVillageMarketShare(site_loc, village)
        # 4. Village Market Share
        district_ms = await districtMarketShare(district_geometry)

        # Convert the dictionary to the desired list format
        district_ms_json = json.dumps([
            {"name": name, "value": round(value * 100, 2)}
            for name, value in district_ms.items()
        ])

        # #9. Get Population
        population, population_8000m, total_coverage_population = await process_population_data(site_loc, comid_city_8000m, elevationProfile)

        # print('to_this_point')

        # #11. Get Signal level, #12. Get Building, #13. Get Road Buffer, #14. Get Closeness
        signal_level,signal_level_data, building, road_line, road_buffer, closeness, poi= await process_city_data(site_loc, city_ids, elevationProfile)
        signal_level['name'] = 'operator'

        poi_count = poi.groupby(['category','main_color']).size().reset_index(name="count")
        poi_count = poi_count.sort_values(by='count', ascending=False).reset_index(drop=True)

        poi_count['category'] = poi_count['category'].apply(capitalize_first_letter)

        # ############# CALCULTATE OPERATOR ################
        optResult, spatialResult = await mainOpt(colopriming_site, site_loc, coloTowerCoverageRadius, towerCoverageRadius, elevationProfileCoverage, population, village_market_share, population_8000m, opt_tenant, tenant_revenue, building, road_buffer, closeness, road_line, line_sectoral, sectoral_elevation_profile, poi, signal_level_data,district_ms, isSiro)
        # print(f"==>> spatialResult: {spatialResult[0][colopriming_site['operator'][0]].keys()}")
        spatial = {}

        for g, key in enumerate(spatialResult[0][colopriming_site['operator'][0]].keys()):
            if key not in  ["sectoral_closeness","sectoral_distance","sectoral_elevation_profile"]:
                geojson = spatialResult[0][colopriming_site['operator'][0]][key].to_json()
                with open(f"data_{key}_{colopriming_site['site_id']}.geojson", "w") as file:
                    json.dump(json.loads(geojson), file, indent=4)

                spatial[key] = json.loads(geojson)


        site_loc_siro = Point(siro_status["tp_long"], siro_status["tp_lat"])

        comid_city_2500m_siro = await getComidCity2500m(site_loc_siro)
        city_ids_siro = comid_city_2500m_siro
        city_ids_str_siro = "'" + "','".join(str(num) for num in city_ids_siro) + "'"

        if len(comid_city_2500m_siro)>1:
            comid_city_2500m_siro = '_'.join(map(str, comid_city_2500m_siro))
        else:
            comid_city_2500m_siro = str(comid_city_2500m_siro[0])

        comid_city_8000m_siro = await genComidCity8000m(site_loc_siro)

        opt_tenant_siro = []
        tenant_revenue_siro = {'TSEL': 0, 'XL': 0, 'IOH': 0, 'SF': 0}
        colo_antena_height_siro = 0

        maxTowerCoverageRadius_siro = CoverageRadius(siro_status['tp_tower_height'][0])
        maxTowerCoverageBufferArea_siro = drawBuffer(siro_status["tp_long"][0], siro_status["tp_lat"][0], maxTowerCoverageRadius_siro)

        towerCoverageRadius_siro = CoverageRadius(siro_status["tp_tower_height"][0])

        coloTowerCoverageRadius_siro = CoverageRadius(siro_status["tp_tower_height"][0])

        elevationProfile_siro = await sectoralElevationProfile(siro_status["tp_long"][0], siro_status["tp_lat"][0], siro_status["tp_tower_height"][0], 2, 180, 0.5, maxTowerCoverageRadius_siro, maxTowerCoverageBufferArea_siro)

        elevationProfileCoverage_siro, line_sectoral_siro, sectoral_elevation_profile_siro, elevationProfileinfo_siro = elevationProfile_siro

        # # 1. Village
        village_siro, household_consumption_siro, pdrb_siro, comid_district_siro = await getVillageHcPdrb(site_loc_siro)
        # # 2. District
        district_competition_siro, district_competition_json_siro, operator_list_siro, district_geometry_siro= await districtCompetition(site_loc_siro, comid_district_siro)
        # # 3. District Market Share
        village_market_share_siro = await getVillageMarketShare(site_loc_siro, village_siro)
        # # 4. Village Market Share
        district_ms_siro = await districtMarketShare(district_geometry_siro)

        # # Convert the dictionary to the desired list format
        district_ms_json_siro = json.dumps([
            {"name": name, "value": round(value * 100, 2)}
            for name, value in district_ms_siro.items()
        ])

        # # #9. Get Population
        population_siro, population_8000m_siro, total_coverage_population_siro = await process_population_data(site_loc_siro, comid_city_8000m_siro, elevationProfile_siro)

        # # print('to_this_point')

        # # #11. Get Signal level, #12. Get Building, #13. Get Road Buffer, #14. Get Closeness
        signal_level_siro,signal_level_data_siro, building_siro, road_line_siro, road_buffer_siro, closeness_siro, poi_siro= await process_city_data(site_loc_siro, city_ids_siro, elevationProfile_siro)
        signal_level_siro['name'] = 'operator'

        poi_count_siro = poi_siro.groupby(['category','main_color']).size().reset_index(name="count")
        poi_count_siro = poi_count_siro.sort_values(by='count', ascending=False).reset_index(drop=True)

        poi_count_siro['category'] = poi_count_siro['category'].apply(capitalize_first_letter)


        siro_site = siro_status[['tenant','tp_tower_height','tp_distance']]
        siro_site.columns = ['operator','tower_height','tp_distance']
        # # # ############# CALCULTATE OPERATOR ################
        optResult_siro, spatialResult_siro = await mainOptSiro(siro_site, site_loc_siro, coloTowerCoverageRadius_siro, towerCoverageRadius_siro, elevationProfileCoverage_siro, population_siro, village_market_share_siro, population_8000m_siro, opt_tenant_siro, tenant_revenue_siro, building_siro, road_buffer_siro, closeness_siro, road_line_siro, line_sectoral_siro, sectoral_elevation_profile_siro, poi_siro, signal_level_data_siro,district_ms_siro)
        # # print(f"==>> spatialResult: {spatialResult[0][colopriming_site['operator'][0]].keys()}")
        competitor_profile = {}
        competitor_profile['competitor_name'] = siro_status['tp_name'][0]
        competitor_profile['competitor_site_id'] = siro_status['tp_site_id'][0]
        competitor_profile['competitor_site_name'] = siro_status['tp_site_name'][0]
        competitor_profile['competitor_tower_height'] = siro_status['tp_tower_height'][0]
        competitor_profile['competitor_site_type'] = siro_status['tp_site_type'][0]
        competitor_profile['competitor_distance'] = siro_status['tp_distance'][0]
        competitor_profile['competitor_longitude'] = siro_status['tp_long'][0]
        competitor_profile['competitor_latitude'] = siro_status['tp_lat'][0]
        competitor_profile['siro_status'] = siro_status['siro_status'][0]
        competitor_profile['competition_type'] = siro_status['competition_type'][0]
        competitor_profile['competition_type_id'] = siro_status['competition_type_id'][0]

        for param in ['total_population','total_revenue','market_share']:
            if optResult[0][param] > optResult_siro[0][param]:
                competitor_profile[param] = 'win'
            else:
                competitor_profile[param] = 'lose'

        if colopriming_site['tower_height'] > siro_status['tp_tower_height'][0]-2 and colopriming_site['tower_height'] < siro_status['tp_tower_height'][0]+2:
            competitor_profile['tower_height'] = 'same'
        elif colopriming_site['tower_height'] > siro_status['tp_tower_height'][0]:
            competitor_profile['tower_height'] = 'win'
        else:
            competitor_profile['tower_height'] = 'lose'

        spatial_siro = {}

        for g, key in enumerate(spatialResult_siro[0][siro_site['operator'][0]].keys()):
            if key not in  ["sectoral_closeness","sectoral_distance","sectoral_elevation_profile"]:
                geojson = spatialResult_siro[0][siro_site['operator'][0]][key].to_json()
                with open(f"data_siro_{key}_{colopriming_site['site_id']}.geojson", "w") as file:
                    json.dump(json.loads(geojson), file, indent=4)

                spatial_siro[key] = json.loads(geojson)

        optResult[0]['market_share_district'] = json.loads(district_ms_json)
        optResult_siro[0]['market_share_district'] = json.loads(district_ms_json_siro)

        update = {"status": "SUCCESS","analysis_site_result":optResult[0],"spatial_site_result":spatial, 'siro_profile':competitor_profile, 'analysis_siro_result':optResult_siro[0],'spatial_siro_result':spatial_siro}
        await update_record(pSiro,record_id, update)

        return {"status": "SUCCESS","result":optResult[0],"spatial":spatial, 'siro_profile':competitor_profile, 'resultSiro':optResult_siro[0],'spatial_siro':spatial_siro}

    except ValidationError as e:
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        status = "FAILURE"
        if running_type == 'background':
            update = {'status': status, 'error_message': tb_str}
            await update_record(pSiro,record_id, update)

            celery_log.info(f"==>> task.status: {status}")
            celery_log.info(f"==>> CELERY ERROR: {tb_str}")
        elif running_type == 'direct':
            update = {'status': status, 'error_message': tb_str}
            await update_record(pSiro,record_id, update)
        else:
            print(f"==>> status: {status}")


        raise HTTPException(status_code=500, detail=f'VALIDATION ERROR: {e}\n{tb_str}')

    except Exception as e:
        status = "FAILURE"
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        if running_type == 'background':
            update = {'status': status, 'error_message': tb_str}
            await update_record(pSiro,record_id, update)
            celery_log.info(f"==>> task.status: {status}")
            celery_log.info(f"==>> CELERY ERROR: {tb_str}")
        elif running_type == 'direct':
            update = {'status': status, 'error_message': tb_str}
            await update_record(pSiro,record_id, update)
            print(f"==>> status: {status}")
        else:
            print(f"==>> status: {status}")

        raise HTTPException(status_code=500, detail=f'ERROR: {e}\n{tb_str}')

    finally:
        if running_type == 'background':
            update = {'finished_at': datetime.now()}
            await update_record(pSiro,record_id, update)
            celery_log.info(f"==>> status: {status}")
        elif running_type == 'direct':
            update = {'finished_at': datetime.now()}
            await update_record(pSiro,record_id, update)
            print(f"==>> status: {status}")
        else:
            print(f"==>> status: {status}")

