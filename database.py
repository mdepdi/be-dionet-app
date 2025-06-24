import asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy import  create_engine, Column, String, Float, Integer, String, DateTime, ForeignKey, Boolean, Numeric, BigInteger

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import delete
from dotenv import load_dotenv
from geoalchemy2 import Geometry
import os
from datetime import datetime
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid

load_dotenv()
DATABASE_URL_ASYNC = os.environ.get('DATABASE_URL_ASYNC')
DATABASE_URL = os.environ.get('DATABASE_URL')

engine = create_async_engine(DATABASE_URL_ASYNC, echo=False)
engineSync = create_engine(DATABASE_URL, echo=False)

SessionSync = sessionmaker(autocommit=False, autoflush=False, bind=engineSync)


SessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

Base = declarative_base()
class mVillage(Base):
    __tablename__ = "m_village"
    comid_village=Column(String, primary_key=True)
    comid_district=Column(String)
    comid_city=Column(String)
    comid_province=Column(String)
    id_village= Column(String)
    id_district= Column(String)
    id_city= Column(String)
    id_province= Column(String)
    village=Column(String)
    district=Column(String)
    city= Column(String)
    province=Column(String)
    household_consumption=Column(Float)
    monthly_pdrb=Column(Float)
    annually_pdrb=Column(Float)
    geometry=Column(Geometry('GEOMETRY', srid=4326, spatial_index=True), nullable=False)

class mCity(Base):
    __tablename__ = "m_city"
    comid_city=Column(String, primary_key=True)
    comid_province=Column(String)
    id_city= Column(String)
    id_province= Column(String)
    city= Column(String)
    province=Column(String)
    island=Column(String)
    geometry=Column(Geometry('GEOMETRY', srid=4326, spatial_index=True), nullable=False)

class mOperator(Base):
    __tablename__ = "m_operator"
    id = Column(Integer, autoincrement=True, primary_key=True)
    site_id=Column(String)
    site_name=Column(String)
    operator=Column(String)
    source_db=Column(String)
    code_db=Column(String)
    longitude=Column(Float)
    latitude=Column(Float)
    tower_height=Column(Float)
    actual_revenue=Column(Float)
    active=Column(Boolean, default=True)
    geometry=Column(Geometry('GEOMETRY', srid=4326, spatial_index=True), nullable=False)

class mRwi(Base):
    __tablename__ = "m_rwi"
    id = Column(Integer, autoincrement=True, primary_key=True)
    rwi = Column(Float)
    comid_city = Column(String)
    active=Column(Boolean, default=True)
    geometry=Column(Geometry('GEOMETRY', srid=4326, spatial_index=True), nullable=False)

class mColopriming(Base):
    __tablename__ = "m_colopriming"
    id=Column(Integer, autoincrement=True, primary_key=True)
    site_id = Column(String)
    site_name=Column(String)
    latitude=Column(Float)
    longitude=Column(Float)
    address=Column(String)
    comid_city=Column(Integer)
    id_city=Column(Integer)
    id_province=Column(Integer)
    city=Column(String)
    province=Column(String)
    site_type=Column(String)
    tower_type=Column(String)
    tower_height=Column(Float)
    antena_height=Column(Float)
    region_area=Column(String)
    region=Column(String)
    status=Column(String)
    tenant=Column(String)
    actual_revenue=Column(String)
    grade=Column(String)
    comid_city_2500m=Column(String)
    comid_province_2500m=Column(String)
    date_updated=Column(DateTime)
    active=Column(Boolean, default=True)
    geometry=Column(Geometry('GEOMETRY', srid=4326, spatial_index=True), nullable=True)


class mColoprimingSopt(Base):
    __tablename__ = "m_colopriming_sopt"
    id=Column(Integer, autoincrement=True, primary_key=True)
    site_id = Column(String)
    site_name=Column(String)
    latitude=Column(Float)
    longitude=Column(Float)
    address=Column(String)
    comid_village=Column(String)
    comid_district=Column(String)
    comid_city=Column(String)
    comid_province=Column(String)
    id_village=Column(String)
    id_district=Column(String)
    id_city=Column(String)
    id_province=Column(String)
    village=Column(String)
    district=Column(String)
    city=Column(String)
    province=Column(String)
    site_type=Column(String)
    tower_type=Column(String)
    tower_height=Column(Float)
    antena_height=Column(Float)
    region_area=Column(String)
    region=Column(String)
    status=Column(String)
    tenant=Column(String)
    actual_revenue=Column(Float)
    start_rental= Column(String)
    end_rental= Column(String)
    date_updated=Column(DateTime)
    active=Column(Boolean, default=True)
    geometry=Column(Geometry('GEOMETRY', srid=4326, spatial_index=True), nullable=True)

class mSiro(Base):
    __tablename__ = "m_siro"
    id=Column(Integer, autoincrement=True, primary_key=True)
    site_id = Column(String)
    site_name=Column(String)
    latitude=Column(Float)
    longitude=Column(Float)
    address=Column(String)
    site_type=Column(String)
    tower_type=Column(String)
    tower_height=Column(Float)
    antena_height=Column(Float)
    region_area=Column(String)
    region=Column(String)
    status=Column(String)
    tenant=Column(String)
    actual_revenue=Column(Float)
    start_rental= Column(String)
    end_rental= Column(String)
    comid_village=Column(String)
    comid_district=Column(String)
    comid_city=Column(String)
    comid_province=Column(String)
    id_village=Column(String)
    id_district=Column(String)
    id_city=Column(String)
    id_province=Column(String)
    village=Column(String)
    district=Column(String)
    city=Column(String)
    province=Column(String)
    date_updated=Column(DateTime)
    active=Column(Boolean, default=True)
    tp_name=Column(String)
    tp_site_id=Column(String)
    tp_site_name=Column(String)
    tp_tower_height=Column(Float)
    tp_antena_height=Column(Float)
    tp_site_type=Column(String)
    tp_distance=Column(Float)
    tp_long=Column(Float)
    tp_lat=Column(Float)
    siro_status=Column(String)
    tp_to_dbo_distance=Column(Float)
    tp_dbo_distance_ratio=Column(Float)
    competition_type=Column(String)
    dbo_distance=Column(Float)
    competition_type_id=Column(String)
    geometry=Column(Geometry('GEOMETRY', srid=4326, spatial_index=True), nullable=True)

class mPoi(Base):
    __tablename__ = "m_poi"
    id = Column(Integer, autoincrement=True, primary_key=True)
    poi_name= Column(String)
    poi_category= Column(String)
    comid_city=Column(String)
    longitude = Column(Float)
    latitude= Column(Float)
    active=Column(Boolean, default=True)
    geometry= Column(Geometry('GEOMETRY', srid=4326, spatial_index=True), nullable=True)

class mMarketShare(Base):
    __tablename__ = "m_market_share"
    id = Column(Integer, autoincrement=True, primary_key=True)
    comid_province=Column(String)
    comid_city    =Column(String)
    comid_district=Column(String)
    comid_village =Column(String)
    id_province   =Column(String)
    id_city       =Column(String)
    id_district   =Column(String)
    id_village    =Column(String)
    province      =Column(String)
    city          =Column(String)
    district      =Column(String)
    village       =Column(String)
    ms_type_1     =Column(String)
    ms_type_2     =Column(String)
    ms_type_3     =Column(String)
    ms_type_4     =Column(String)
    ms_type_5     =Column(String)
    ms_type_6     =Column(String)
    ms_type_7     =Column(String)
    ms_type_8     =Column(String)
    ms_type_9     =Column(String)
    ms_type_10    =Column(String)
    ms_type_11    =Column(String)
    ms_type_12    =Column(String)
    active = Column(Boolean, default=True)

class mInternetSpeedTest(Base):
    __tablename__ = "m_internet_speed_test"
    id = Column(Integer, autoincrement=True, primary_key=True)
    comid_city      =Column(String)
    quadkey         =Column(String)
    avg_d_kbps      =Column(Integer)
    avg_u_kbps      =Column(Integer)
    avg_lat_ms      =Column(Integer)
    tests           =Column(Integer)
    devices         =Column(Integer)
    active = Column(Boolean, default=True)
    geometry        = Column(Geometry('GEOMETRY', srid=4326, spatial_index=True), nullable=True)

class mMobileInternetSpeed(Base):
    __tablename__ = "m_mobile_internet_speed"
    id = Column(Integer, autoincrement=True, primary_key=True)
    comid_city      =Column(String)
    quadkey         =Column(String)
    avg_d_kbps      =Column(Integer)
    avg_u_kbps      =Column(Integer)
    avg_lat_ms      =Column(Integer)
    tests           =Column(Integer)
    devices         =Column(Integer)
    active = Column(Boolean, default=True)
    geometry        = Column(Geometry('GEOMETRY', srid=4326, spatial_index=True), nullable=True)
class mFixedInternetSpeed(Base):
    __tablename__ = "m_fixed_internet_speed"
    id = Column(Integer, autoincrement=True, primary_key=True)
    comid_city      =Column(String)
    quadkey         =Column(String)
    avg_d_kbps      =Column(Integer)
    avg_u_kbps      =Column(Integer)
    avg_lat_ms      =Column(Integer)
    tests           =Column(Integer)
    devices         =Column(Integer)
    active = Column(Boolean, default=True)
    geometry        = Column(Geometry('GEOMETRY', srid=4326, spatial_index=True), nullable=True)

class mArpuAf(Base):
    __tablename__ = "m_arpu_af"
    id = Column(Integer, autoincrement=True, primary_key=True)
    comid_city      =Column(String)
    operator        =Column(String)
    id_operator     =Column(Integer)
    code             =Column(String)
    longitude       =Column(Float)
    latitude        =Column(Float)
    arpu             =Column(Float)
    area_factor     =Column(Float)
    active = Column(Boolean, default=True)
    geometry        = Column(Geometry('GEOMETRY', srid=4326, spatial_index=True), nullable=True)

class mBuildToSuit(Base):
    __tablename__ = "m_build_to_suit"
    id = Column(Integer, autoincrement=True, primary_key=True)
    site_id = Column(String)
    tower_height = Column(Float)
    longitude = Column(Float)
    latitude = Column(Float)
    address = Column(String)
    created_at = Column(DateTime)
    active = Column(Boolean, default=True)

class pColopriming(Base):
    __tablename__ = "p_colopriming"
    project_id = Column(Integer, autoincrement=True, primary_key=True)
    task_id = Column(String)
    status = Column(String)
    site_id = Column(String)
    tower_height = Column(Float)
    antena_height = Column(Float)
    type = Column(String)
    site_type = Column(String)
    longitude = Column(Float)
    latitude = Column(Float)
    coverage_profile = Column(String)
    economy_profile = Column(String)
    netspeed_profile = Column(String)
    signal_profile = Column(String)
    competition_profile = Column(String)
    market_share_district = Column(String)
    nearest_colopriming = Column(String)
    poi_profile = Column(String)
    colopriming_status = Column(String)
    created_at = Column(DateTime)
    finished_at = Column(DateTime)
    error_message = Column(String)
    active = Column(Boolean, default=True)

class pSiro(Base):
    __tablename__ = "p_siro"
    project_id = Column(Integer, autoincrement=True, primary_key=True)
    task_id = Column(String)
    status = Column(String)
    site_id = Column(String)
    operator = Column(String)
    type = Column(String)
    analysis_site_result = Column(JSONB)
    spatial_site_result = Column(JSONB)
    siro_profile = Column(JSONB)
    analysis_siro_result = Column(JSONB)
    spatial_siro_result = Column(JSONB)
    created_at = Column(DateTime)
    finished_at = Column(DateTime)
    error_message = Column(String)
    active = Column(Boolean, default=True)


class pdColopriming(Base):
    __tablename__ = "pd_colopriming"
    id = Column(Integer, autoincrement=True, primary_key=True)
    project_id = Column(Integer)
    site_id = Column(String)
    tower_height = Column(Float)
    antena_height = Column(Float)
    operator = Column(String)
    gridness_ratio = Column(Float)
    gridness_quadran = Column(Integer)
    total_population = Column(Integer)
    market_share_district = Column(String)
    market_share_village = Column(String)
    market_share_surrounding = Column(String)
    market_share_site = Column(String)
    market_share = Column(Float)
    area_factor = Column(Float)
    arpu = Column(Float)
    total_revenue=Column(Float)
    grade_revenue=Column(String)
    colopriming_status=Column(Integer)
    overall_grade=Column(String)
    potential_collocation=Column(String)
    avg_distance=Column(Float)
    avg_distance_opt=Column(Float)
    surrounding_quality=Column(Integer)
    quadran_area_m2=Column(Float)
    building_count=Column(Integer)
    building_area_m2=Column(Float)
    road_length_m=Column(Float)
    road_area_m2=Column(Float)
    closeness_score=Column(Float)
    built_area_m2=Column(Float)
    built_area_ratio=Column(Float)
    min_sectoral_dist=Column(Float)
    max_sectoral_dist=Column(Float)
    signal_strength=Column(Float)
    los_profile=Column(String)
    active = Column(Boolean, default=True)

class pdsColopriming(Base):
    __tablename__ = "pds_colopriming"
    id = Column(Integer, autoincrement=True, primary_key=True)
    project_id = Column(Integer)
    site_id = Column(String)
    tower_height = Column(Float)
    antena_height = Column(Float)
    operator = Column(String)
    kuadran = Column(Integer)
    azimuth_start = Column(Integer)
    azimuth_end = Column(Integer)
    total_population = Column(Integer)
    total_revenue=Column(Float)
    building_count=Column(Integer)
    building_area_m2=Column(Float)
    road_length_m=Column(Float)
    road_area_m2=Column(Float)
    closeness_score=Column(Float)
    sectoral_area_m2=Column(Float)
    built_area_m2=Column(Float)
    built_area_ratio=Column(Float)
    min_sectoral_dist=Column(Float)
    max_sectoral_dist=Column(Float)
    poi_count=Column(Integer)
    active = Column(Boolean, default=True)

class spCoverageRadius(Base):
    __tablename__ = "sp_coverage_radius"
    id = Column(Integer, autoincrement=True, primary_key=True)
    project_id = Column(Integer)
    site_id = Column(String)
    tower_height = Column(Float)
    antena_height = Column(Float)
    area = Column(Float)
    geometry = Column(Geometry('GEOMETRY', srid=4326, spatial_index=True), nullable=False)

class spQuadranSec(Base):
    __tablename__ = "sp_quadran_sec"
    id = Column(Integer, autoincrement=True, primary_key=True)
    project_id = Column(Integer)
    site_id = Column(String)
    tower_height = Column(Float)
    antena_height = Column(Float)
    operator = Column(String)
    kuadran = Column(Integer)
    azimuth_start = Column(Integer)
    azimuth_end = Column(Integer)
    distance = Column(Float)
    area = Column(Float)
    geometry = Column(Geometry('GEOMETRY', srid=4326, spatial_index=True), nullable=False)

class spBuildingSec(Base):
    __tablename__ = "sp_building_sec"
    id = Column(Integer, autoincrement=True, primary_key=True)
    project_id = Column(Integer)
    site_id = Column(String)
    tower_height = Column(Float)
    antena_height = Column(Float)
    operator = Column(String)
    kuadran = Column(Integer)
    count = Column(Integer)
    area = Column(Float)
    geometry = Column(Geometry('GEOMETRY', srid=4326, spatial_index=True), nullable=False)

class spRoadSec(Base):
    __tablename__ = "sp_road_sec"
    id = Column(Integer, autoincrement=True, primary_key=True)
    project_id = Column(Integer)
    site_id = Column(String)
    tower_height = Column(Float)
    antena_height = Column(Float)
    operator = Column(String)
    kuadran = Column(Integer)
    length = Column(Float)
    area = Column(Float)
    geometry = Column(Geometry('GEOMETRY', srid=4326, spatial_index=True), nullable=False)

class spRoadCloseness(Base):
    __tablename__ = "sp_road_closeness"
    id = Column(Integer, autoincrement=True, primary_key=True)
    project_id = Column(Integer)
    site_id = Column(String)
    tower_height = Column(Float)
    antena_height = Column(Float)
    operator = Column(String)
    kuadran = Column(Integer)
    closeness = Column(Float)
    geometry = Column(Geometry('GEOMETRY', srid=4326, spatial_index=True), nullable=False)

class spSectoralDistance(Base):
    __tablename__ = "sp_sec_distance"
    id = Column(Integer, autoincrement=True, primary_key=True)
    project_id = Column(Integer)
    site_id = Column(String)
    tower_height = Column(Float)
    antena_height = Column(Float)
    operator = Column(String)
    kuadran = Column(Integer)
    line_id = Column(Integer)
    distance = Column(Float)
    graph = Column(String)
    geometry = Column(Geometry('GEOMETRY', srid=4326, spatial_index=True), nullable=False)

class spSectoralElevation(Base):
    __tablename__ = "sp_sec_elevation"
    id = Column(Integer, autoincrement=True, primary_key=True)
    project_id = Column(Integer)
    site_id = Column(String)
    tower_height = Column(Float)
    antena_height = Column(Float)
    operator = Column(String)
    kuadran = Column(Integer)
    line_id = Column(Integer)
    distance = Column(Float)
    max_distance = Column(Float)
    ground_height = Column(Float)
    signal_height = Column(Float)
    covered = Column(Integer)
    geometry = Column(Geometry('GEOMETRY', srid=4326, spatial_index=True), nullable=False)


class spSectoralPoi(Base):
    __tablename__ = "sp_sec_poi"
    id = Column(Integer, autoincrement=True, primary_key=True)
    project_id = Column(Integer)
    site_id = Column(String)
    tower_height = Column(Float)
    antena_height = Column(Float)
    kuadran = Column(Integer)
    poi_id = Column(String)
    updatetime = Column(String)
    confidence = Column(Float)
    category = Column(String)
    sub_category = Column(String)
    name = Column(String)
    website = Column(String)
    phone = Column(String)
    source = Column(String)
    color = Column(String)
    main_color = Column(String)
    istsel = Column(Integer)
    isxl = Column(Integer)
    isioh = Column(Integer)
    issf = Column(Integer)
    distance = Column(Float)
    geometry = Column(Geometry('GEOMETRY', srid=4326, spatial_index=True), nullable=False)


class pBulkJob(Base):
    __tablename__ = "p_bulk_job"
    job_id = Column(String, primary_key=True)  # UUID
    task_id = Column(String)  # Celery task ID
    job_name = Column(String, nullable=False)
    file_name = Column(String, nullable=False)
    total_sites = Column(Integer, default=0)
    processed_sites = Column(Integer, default=0)
    failed_sites = Column(Integer, default=0)
    status = Column(String, default="PENDING")  # PENDING, PROCESSING, SUCCESS, FAILURE
    progress_percentage = Column(Float, default=0.0)
    result_file_path = Column(String)
    error_message = Column(String)
    created_at = Column(DateTime, default=datetime.now)
    started_at = Column(DateTime)
    finished_at = Column(DateTime)
    created_by = Column(String)  # Could be API key or user identifier
    active = Column(Boolean, default=True)


class pBulkJobDetail(Base):
    __tablename__ = "p_bulk_job_detail"
    id = Column(Integer, autoincrement=True, primary_key=True)
    job_id = Column(String, ForeignKey("p_bulk_job.job_id"), nullable=False)
    site_id = Column(String, nullable=False)
    site_index = Column(Integer, nullable=False)  # Order in the file
    status = Column(String, default="PENDING")  # PENDING, PROCESSING, SUCCESS, FAILURE
    project_id = Column(Integer)  # Links to actual colopriming project if successful
    error_message = Column(String)
    processed_at = Column(DateTime)
    active = Column(Boolean, default=True)

    # Site data for processing
    longitude = Column(Float)
    latitude = Column(Float)
    tower_height = Column(Float)
    antena_height = Column(Float)
    site_type = Column(String)
    operator = Column(String)
