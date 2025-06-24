from fastapi import HTTPException, status, Response
from fastapi.responses import JSONResponse
import pandas as pd
import geopandas as gpd
from database import *
from geoalchemy2 import WKTElement
from sqlalchemy.inspection import inspect
from enum import Enum
import logging
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def import_m_village(db):
    data = gpd.read_parquet(f'./data/tb_village.parquet')
    # data = gpd.read_parquet(f'{google_cloud_storage_uri}/tb_village.parquet', filesystem=fs)
    for index, row in tqdm(data.iterrows(), total=len(data)):
        try:
            geometry_wkt = row['geometry'].wkt if row['geometry'] else None
            if geometry_wkt:
                geometry_element = WKTElement(geometry_wkt, srid=4326)
            else:
                geometry_element = None

            model = mVillage(
                comid_village=str(row['comid_village']),
                comid_district=str(row['comid_district']),
                comid_city=str(row['comid_city']),
                comid_province=str(row['comid_province']),
                id_village= str(row['id_village']),
                id_district= str(row['id_district']),
                id_city= str(row['id_city']),
                id_province= str(row['comid_province']),
                village=str(row['village']),
                district=str(row['district']),
                city= str(row['city']),
                province=str(row['province']),
                household_consumption=str(row['household_consumption']),
                monthly_pdrb=str(row['monthly_pdrb']),
                annually_pdrb=str(row['annually_pdrb']),
                geometry=geometry_element
            )
            db.add(model)
        except Exception as e:
            print(f"==>> e: {e}")
            pass
        finally:
            db.commit()
def import_m_city(db):
    data = gpd.read_parquet(f'./data/tb_city.parquet')
    # data = gpd.read_parquet(f'{google_cloud_storage_uri}/tb_city.parquet', filesystem=fs)
    print(f"==>> data: {data}")
    for index, row in tqdm(data.iterrows(), total=len(data)):
        try:
            geometry_wkt = row['geometry'].wkt if row['geometry'] else None
            if geometry_wkt:
                geometry_element = WKTElement(geometry_wkt, srid=4326)
            else:
                geometry_element = None

            model = mCity(
                comid_city=str(row['comid_city']),
                comid_province=str(row['comid_province']),
                id_city= str(row['id_city']),
                id_province= str(row['comid_province']),
                city= str(row['city']),
                province=str(row['province']),
                island =str(row['island']),
                geometry=geometry_element
            )
            db.add(model)
        except Exception as e:
            print(f"==>> e: {e}")
            pass
        finally:
            db.commit()

def import_m_rwi(db):
    data = gpd.read_parquet(f'./data/tb_rwi.parquet')
    # data = gpd.read_parquet(f'{google_cloud_storage_uri}/tb_rwi.parquet', filesystem=fs)
    for index, row in tqdm(data.iterrows(), total=len(data)):
        try:
            geometry_wkt = row['geometry'].wkt if row['geometry'] else None
            if geometry_wkt:
                geometry_element = WKTElement(geometry_wkt, srid=4326)
            else:
                geometry_element = None

            model = mRwi(
                rwi=float(row['rwi']),
                comid_city=str(row['comid_city']),
                active= True,
                geometry=geometry_element
            )
            db.add(model)
        except Exception as e:
            print(f"==>> e: {e}")
            pass
        finally:
            db.commit()

def import_m_colopriming(db):
    data = gpd.read_parquet(f'./data/tb_colopriming.parquet')
    # data = gpd.read_parquet(f'{google_cloud_storage_uri}/tb_colopriming.parquet', filesystem=fs)
    for index, row in tqdm(data.iterrows(), total=len(data)):
        try:
            geometry_wkt = row['geometry'].wkt if row['geometry'] else None
            if geometry_wkt:
                geometry_element = WKTElement(geometry_wkt, srid=4326)
            else:
                geometry_element = None

            model = mColopriming(
                site_id=str(row['site_id']),
                site_name=str(row['site_name']),
                latitude=float(row['latitude']),
                longitude=float(row['longitude']),
                address=str(row['address']),
                id_city=int(row['id_city']),
                comid_city=int(row['comid_city']),
                id_province=int(row['id_province']),
                city=str(row['city']),
                province=str(row['province']),
                site_type=str(row['site_type']),
                tower_type=str(row['tower_type']),
                tower_height=float(row['tower_height']),
                antena_height=float(row['tower_height']),
                region_area=str(row['region_area']),
                region=str(row['region']),
                status=str(row['status']),
                tenant=str(row['tenant']),
                actual_revenue=str(row['revenue']),
                grade=str(row['grade']),
                comid_city_2500m=str(row['comid_city_2500m']),
                comid_province_2500m=str(row['comid_province_2500m']),
                date_updated=str(row['date_updated']),
                active=True,
                geometry=geometry_element
            )
            db.add(model)
        except Exception as e:
            print(f"==>> e: {e}")
            pass
        finally:
            db.commit()




def import_m_colopriming_sopt(db):
    data = gpd.read_parquet(f'./data/tb_colopriming_sopt.parquet')
    # data = gpd.read_parquet(f'{google_cloud_storage_uri}/tb_colopriming.parquet', filesystem=fs)
    for index, row in tqdm(data.iterrows(), total=len(data)):
        try:
            geometry_wkt = row['geometry'].wkt if row['geometry'] else None
            if geometry_wkt:
                geometry_element = WKTElement(geometry_wkt, srid=4326)
            else:
                geometry_element = None


            model = mColoprimingSopt(
                site_id=str(row['site_id']),
                site_name=str(row['site_name']),
                latitude=float(row['latitude']),
                longitude=float(row['longitude']),
                address=str(row['address']),
                id_village=str(row['id_village']),
                id_district=str(row['id_district']),
                id_city=str(row['id_city']),
                id_province=str(row['id_province']),
                comid_village=str(row['comid_village']),
                comid_district=str(row['comid_district']),
                comid_city=str(row['comid_city']),
                comid_province=str(row['comid_province']),
                village=str(row['village']),
                district=str(row['district']),
                city=str(row['city']),
                province=str(row['province']),
                site_type=str(row['site_type']),
                tower_type=str(row['tower_type']),
                tower_height=float(row['tower_height']),
                antena_height=float(row['tower_height']),
                region_area=str(row['region_area']),
                region=str(row['region']),
                status=str(row['status']),
                tenant=str(row['tenant']),
                actual_revenue=float(row['revenue']),
                start_rental=str(row['start_rental']),
                end_rental=str(row['end_rental']),
                date_updated=str(row['date_updated']),
                active=True,
                geometry=geometry_element
            )
            db.add(model)
        except Exception as e:
            print(f"==>> e: {e}")
            raise
        finally:
            db.commit()




def import_m_operator(db):
    data = gpd.read_parquet(f'./data/tb_operator.parquet')
    # data = gpd.read_parquet(f'{google_cloud_storage_uri}/tb_operator.parquet', filesystem=fs)
    for index, row in tqdm(data.iterrows(), total=len(data)):
        try:
            geometry_wkt = row['geometry'].wkt if row['geometry'] else None
            if geometry_wkt:
                geometry_element = WKTElement(geometry_wkt, srid=4326)
            else:
                geometry_element = None

            model = mOperator(
                site_id=str(row['site_id']),
                site_name=str(row['site_name']),
                operator=str(row['operator']),
                source_db=str(row['source_db']),
                code_db=str(row['code_db']),
                longitude=float(row['longitude']),
                latitude=float(row['latitude']),
                tower_height=float(row['tower_height']),
                actual_revenue=float(row['actual_revenue']),
                active=True,
                geometry=geometry_element

            )
            db.add(model)
        except Exception as e:
            print(f"==>> e: {e}")
            pass
        finally:
            db.commit()
def import_m_poi(db):
    # data = gpd.read_parquet(f'{google_cloud_storage_uri}/tb_poi.parquet', filesystem=fs)
    data = gpd.read_parquet(f'./data/tb_poi.parquet')
    for index, row in tqdm(data.iterrows(), total=len(data)):
        try:
            geometry_wkt = row['geometry'].wkt if row['geometry'] else None
            if geometry_wkt:
                geometry_element = WKTElement(geometry_wkt, srid=4326)
            else:
                geometry_element = None

            model = mPoi(
                poi_name = str(row['poi_name']),
                poi_category = str(row['poi_category']),
                comid_city = str(row['comid_city']),
                longitude=str(row['longitude']),
                latitude=str(row['latitude']),
                active=True,
                geometry=geometry_element
            )
            db.add(model)
        except Exception as e:
            print(f"==>> e: {e}")
            pass
        finally:
            db.commit()
def import_m_market_share(db):
    data = pd.read_parquet('./data/tb_market_share.parquet')
    for index, row in tqdm(data.iterrows(), total=len(data)):
        try:
            model = mMarketShare(
                comid_province=str(int(row["comid_province"])),
                comid_city    =str(int(row["comid_city"])),
                comid_district=str(int(row["comid_district"])),
                comid_village =str(int(row["comid_village"])),
                id_province   =str(row["id_province"]),
                id_city       =str(row["id_city"]),
                id_district   =str(row["id_district"]),
                id_village    =str(row["id_village"]),
                province      =str(row["province"]),
                city          =str(row["city"]),
                district      =str(row["district"]),
                village       =str(row["village"]),
                ms_type_1     =str(row["ms_type_1"]),
                ms_type_2     =str(row["ms_type_2"]),
                ms_type_3     =str(row["ms_type_3"]),
                ms_type_4     =str(row["ms_type_4"]),
                ms_type_5     =str(row["ms_type_5"]),
                ms_type_6     =str(row["ms_type_6"]),
                ms_type_7     =str(row["ms_type_7"]),
                ms_type_8     =str(row["ms_type_8"]),
                ms_type_9     =str(row["ms_type_9"]),
                ms_type_10    =str(row["ms_type_10"]),
                ms_type_11    =str(row["ms_type_11"]),
                ms_type_12    =str(row["ms_type_12"]),
                active = True
            )
            db.add(model)
        except Exception as e:
            print(f"==>> e: {e}")
            pass
        finally:
            db.commit()
def import_m_internet_speed_test(db):
    data = gpd.read_parquet(f'./data/tb_internet_speed_test.parquet')
    # data = gpd.read_parquet(f'{google_cloud_storage_uri}/tb_internet_speed_test.parquet', filesystem=fs)
    for index, row in tqdm(data.iterrows(), total=len(data)):
        try:
            geometry_wkt = row['geometry'].wkt if row['geometry'] else None
            if geometry_wkt:
                geometry_element = WKTElement(geometry_wkt, srid=4326)
            else:
                geometry_element = None

            model = mInternetSpeedTest(
                comid_city      =str(int(row["comid_city"])),
                quadkey         =str(row["quadkey"]),
                avg_d_kbps      =str(row["avg_d_kbps"]),
                avg_u_kbps      =str(row["avg_u_kbps"]),
                avg_lat_ms      =str(row["avg_lat_ms"]),
                tests           =str(row["tests"]),
                devices         =str(row["devices"]),
                active = True,
                geometry = geometry_element
            )
            db.add(model)
        except Exception as e:
            print(f"==>> e: {e}")
            pass
        finally:
            db.commit()
def import_m_mobile_internet_speed(db):
    data = gpd.read_parquet(f'./data/tb_mobile_internet_speed.parquet')
    # data = gpd.read_parquet(f'{google_cloud_storage_uri}/tb_internet_speed_test.parquet', filesystem=fs)
    for index, row in tqdm(data.iterrows(), total=len(data)):
        try:
            geometry_wkt = row['geometry'].wkt if row['geometry'] else None
            if geometry_wkt:
                geometry_element = WKTElement(geometry_wkt, srid=4326)
            else:
                geometry_element = None

            model = mMobileInternetSpeed(
                comid_city      =str(int(row["comid_city"])),
                quadkey         =str(row["quadkey"]),
                avg_d_kbps      =int(row["avg_d_kbps"]),
                avg_u_kbps      =int(row["avg_u_kbps"]),
                avg_lat_ms      =int(row["avg_lat_ms"]),
                tests           =int(row["tests"]),
                devices         =int(row["devices"]),
                active = True,
                geometry = geometry_element
            )
            db.add(model)
        except Exception as e:
            print(f"==>> e: {e}")
            pass
        finally:
            db.commit()
def import_m_fixed_internet_speed(db):
    data = gpd.read_parquet(f'./data/tb_fixed_internet_speed.parquet')
    for index, row in tqdm(data.iterrows(), total=len(data)):
        try:
            geometry_wkt = row['geometry'].wkt if row['geometry'] else None
            if geometry_wkt:
                geometry_element = WKTElement(geometry_wkt, srid=4326)
            else:
                geometry_element = None

            model = mFixedInternetSpeed(
                comid_city      =str(int(row["comid_city"])),
                quadkey         =str(row["quadkey"]),
                avg_d_kbps      =int(row["avg_d_kbps"]),
                avg_u_kbps      =int(row["avg_u_kbps"]),
                avg_lat_ms      =int(row["avg_lat_ms"]),
                tests           =int(row["tests"]),
                devices         =int(row["devices"]),
                active = True,
                geometry = geometry_element
            )
            db.add(model)
        except Exception as e:
            print(f"==>> e: {e}")
            pass
        finally:
            db.commit()
def import_m_arpu_af(db):
    data = gpd.read_parquet(f'./data/tb_arpu_af.parquet')
    for index, row in tqdm(data.iterrows(), total=len(data)):
        try:
            geometry_wkt = row['geometry'].wkt if row['geometry'] else None
            if geometry_wkt:
                geometry_element = WKTElement(geometry_wkt, srid=4326)
            else:
                geometry_element = None

            model = mArpuAf(
                comid_city      =str(int(row["comid_city"])),
                active = True,
                operator        =str(row["operator"]),
                id_operator     =int(row["id_operator"]),
                code            =str(row["code"]),
                longitude       =float(row["longitude"]),
                latitude        =float(row["latitude"]),
                arpu            =float(row["arpu"]),
                area_factor     =float(row["area_factor"]),
                geometry = geometry_element
            )
            db.add(model)
        except Exception as e:
            print(f"==>> e: {e}")
            pass
        finally:
            db.commit()

def import_m_siro(db):
    data = pd.read_csv(f'./data/siro.csv')
    data = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data.longitude, data.latitude), crs='epsg:4326')

    for _, row in tqdm(data.iterrows(), total=len(data)):
        tp_to_dbo_distance = 0.0 if pd.isna(row["tp_to_dbo_distance"]) or row["tp_to_dbo_distance"] == "" else float(row["tp_to_dbo_distance"])
        tp_dbo_distance_ratio = 0.0 if pd.isna(row["tp_dbo_distance_ratio"]) or row["tp_dbo_distance_ratio"] == "" else float(row["tp_dbo_distance_ratio"])

        if row['tp_tower_height'] != 'No Data':
            tp_tower_height = row['tp_tower_height']
        else:
            tp_tower_height = row['tower_height']


        try:
            # Handle geometry
            geometry_wkt = row['geometry'].wkt if row['geometry'] else None
            geometry_element = WKTElement(geometry_wkt, srid=4326) if geometry_wkt else None

            model = mSiro(
                site_id=str(row["site_id"]),
                site_name=str(row["site_name"]),
                latitude=float(row["latitude"]),
                longitude=float(row["longitude"]),
                address=str(row["address"]),
                site_type=str(row["site_type"]),
                tower_type=str(row["tower_type"]),
                tower_height=float(row["tower_height"]),
                antena_height=float(row["tower_height"]),
                region_area=str(row["region_area"]),
                region=str(row["region"]),
                status=str(row["status"]),
                tenant=str(row["tenant"]),
                actual_revenue=float(row["revenue"]),
                start_rental=str(row["start_rental"]),
                end_rental=str(row["end_rental"]),
                comid_village=str(row["comid_village"]),
                comid_district=str(row["comid_district"]),
                comid_city=str(row["comid_city"]),
                comid_province=str(row["comid_province"]),
                id_village=str(row["id_village"]),
                id_district=str(row["id_district"]),
                id_city=str(row["id_city"]),
                id_province=str(row["id_province"]),
                village=str(row["village"]),
                district=str(row["district"]),
                city=str(row["city"]),
                province=str(row["province"]),
                date_updated=str(row['date_updated']),
                tp_name=str(row["tp_name"]),
                tp_site_id=str(row["tp_site_id"]),
                tp_site_name=str(row["tp_site_name"]),
                tp_tower_height=tp_tower_height,
                tp_antena_height=tp_tower_height,
                tp_site_type=str(row["tp_site_type"]),
                tp_distance=float(row["tp_distance"]),
                tp_long=float(row["tp_long"]),
                tp_lat=float(row["tp_lat"]),
                siro_status=str(row["siro_status"]),
                tp_to_dbo_distance=tp_to_dbo_distance,
                tp_dbo_distance_ratio=tp_dbo_distance_ratio,
                competition_type=str(row["competition_type"]),
                dbo_distance=float(row["dbo_distance"]),
                competition_type_id=str(row["competition_type_id"]),
                geometry=geometry_element
            )
            db.add(model)
        except Exception as e:
            raise
            pass
        finally:
            db.commit()

class tables(str, Enum):
    ALL_TABLES = 'all_tables'
    MASTER_VILLAGE = 'm_village'
    MASTER_CITY = 'm_city'
    MASTER_OPERATOR = 'm_operator'
    MASTER_COLOPRIMING = 'm_colopriming'
    MASTER_COLOPRIMING_SOPT = 'm_colopriming_sopt'
    MASTER_RWI = 'm_rwi'
    MASTER_POI = 'm_poi'
    MASTER_INTERNET_SPEED_TEST = 'm_internet_speed_test'
    MASTER_MOBILE_INTERNET_SPEED = 'm_mobile_internet_speed'
    MASTER_FIXED_INTERNET_SPEED = 'm_fixed_internet_speed'
    MASTER_BUILD_TO_SUIT = 'm_build_to_suit'
    MASTER_SIRO= 'm_siro'
    PROJECT_SIRO = 'p_siro'
    PROJECT_COLOPRIMING = 'p_colopriming'
    PROJECT_DETAIL_COLOPRIMING = 'pd_colopriming'
    PROJECT_DETAIL_SECTORAL_COLOPRIMING = 'pds_colopriming'
    SPATIAL_COVERAGE_RADIUS = 'sp_coverage_radius'
    SPATIAL_QUADRAN_SECTORAL = 'sp_quadran_sec'
    SPATIAL_BUILDING_SECTORAL = 'sp_building_sec'
    SPATIAL_ROAD_SECTROAL = 'sp_road_sec'
    SPATIAL_ROAD_CLOSENESS = 'sp_road_closeness'
    SPATIAL_SECTORAL_DISTANCE = 'sp_sec_distance'
    SPATIAL_SECTORAL_ELEVATION = 'sp_sec_elevation'
    SPATIAL_SECTORAL_POI = 'sp_sec_poi'


tablesModel = {
    'm_city': {"model":mCity,"method":import_m_city},
    'm_village': {"model":mVillage,"method":import_m_village},
    'm_colopriming': {"model":mColopriming,"method":import_m_colopriming},
    'm_colopriming_sopt': {"model":mColoprimingSopt,"method":import_m_colopriming_sopt},
    'm_rwi': {"model":mRwi,"method":import_m_rwi},
    'm_operator': {"model":mOperator,"method":import_m_operator},
    'm_poi': {"model":mPoi,"method":import_m_poi},
    'm_internet_speed_test': {"model":mInternetSpeedTest,"method":import_m_internet_speed_test},
    'm_mobile_internet_speed': {"model":mMobileInternetSpeed,"method":import_m_mobile_internet_speed},
    'm_fixed_internet_speed': {"model":mFixedInternetSpeed,"method":import_m_fixed_internet_speed},
    'm_siro': {"model":mSiro,"method":import_m_siro},
    'p_siro': {"model":pSiro},
    'm_build_to_suit': {"model":mBuildToSuit},
    'p_colopriming': {"model":pColopriming},
    'pd_colopriming': {"model":pdColopriming},
    'pds_colopriming': {"model":pdsColopriming},
    'sp_coverage_radius': {"model":spCoverageRadius},
    'sp_quadran_sec': {"model":spQuadranSec},
    'sp_building_sec': {"model":spBuildingSec},
    'sp_road_sec': {"model":spRoadSec},
    'sp_road_closeness': {"model":spRoadCloseness},
    'sp_sec_distance': {"model":spSectoralDistance},
    'sp_sec_elevation': {"model":spSectoralElevation},
    'sp_sec_poi': {"model":spSectoralPoi},
    'p_bulk_job': {"model":pBulkJob},
    'p_bulk_job_detail': {"model":pBulkJobDetail},
}

def migrate_database(response):
    try:
        inspector = inspect(engineSync)
        for table_name in tablesModel.keys():
            if not inspector.has_table(table_name):
                Base.metadata.tables[table_name].create(engineSync, checkfirst=True)
        response.status_code = status
        return JSONResponse(content={"status": 200, "message": f"Migrate Database successfully"})

    except Exception as e:
        error_message = {
            "status":500,
            "message": f"Failed to migrate database: {e}"
            }
        raise HTTPException(status_code=500, detail=error_message)


def flush_tables(response, table):
    try:
        db = SessionSync()
        if table == 'all_tables':
            for tb in list(reversed(tablesModel.keys())):
                if inspect(engineSync).has_table(tablesModel[tb]['model'].__tablename__):
                    db.query(tablesModel[tb]['model']).delete()
                    db.commit()
                    print(f"==>> tb flushed: {tb}")
        else:
            if inspect(engineSync).has_table(tablesModel[table]['model'].__tablename__):
                db.query(tablesModel[table]['model']).delete()
                db.commit()
        response.status_code = status
        return JSONResponse(content={"status": 200, "message": f"{table} flushed successfully"})
    except Exception as e:
        db.rollback()
        error_message = {
            "status":500,
            "message": f"Failed to flush the {table}: {e}"
            }
        raise HTTPException(status_code=200, detail=error_message)
    finally:
        db.close()

def populate_database(response, table):
    # try:
    db = SessionSync()
    if table == 'all_tables':
        flush_tables(response, 'all_tables')
        for tb in list((tablesModel.keys())):
            try:
                logger.info(f"==>> tb: {tb}")
                try:
                    tablesModel[tb]['method'](db)
                    db.commit()
                except:
                    pass
            except Exception as e:
                db.rollback()
                error_message = {
                    "status":500,
                    "message": f"Failed to populate {table} in database: {e}"
                    }
                raise HTTPException(status_code=500, detail=error_message)
            finally:
                db.close()
    else:
        # flush_tables(response, table)
        tablesModel[table]['method'](db)
        db.commit()

    response.status_code = status
    return JSONResponse(content={"status": 200, "message": f"Populate {table} in Database successfully"})