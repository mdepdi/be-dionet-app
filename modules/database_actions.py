from sqlalchemy import create_engine, Column, String, Float, Integer, String, DateTime, ForeignKey, Boolean, Numeric, BigInteger, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import delete
from dotenv import load_dotenv
from datetime import datetime
from geoalchemy2 import Geometry
import os


from database import *


async def get_records(BaseModel):
    '''
    Retrieve all celery records from the database.

    Returns:
        list: A list of celery records.
    '''
    try:
        async with SessionLocal() as session:
            return await session.query(BaseModel).all()
    except SQLAlchemyError as e:
        error_message = f'Failed to retrieve celery records: {str(e)}'
        raise Exception(error_message)


async def delete_record(BaseModel, record_id: int) -> None:
    '''
    Delete a celery record by ID.

    Args:
        record_id (int): ID of the celery record to be deleted.
    '''
    try:
        async with SessionLocal() as session:
            delete_statement = delete(BaseModel).where(
                BaseModel.project_id == record_id)
            await session.execute(delete_statement)
            await session.commit()
    except SQLAlchemyError as e:
        error_message = f'Failed to delete celery record with ID {record_id}: {str(e)}'
        raise Exception(error_message)


async def update_record(BaseModel, record_id: int, updates: dict) -> None:
    '''
    Update a record by ID with the given updates.

    Args:
        record_id (int): ID of the record to be updated.
        updates (dict): Dictionary of fields and their updated values.
    '''
    try:
        async with SessionLocal() as session:
            result = await session.execute(
                select(BaseModel).where(BaseModel.project_id == record_id)
            )
            db_item = result.scalars().first()
            if db_item:
                for field, value in updates.items():
                    if hasattr(db_item, field):
                        setattr(db_item, field, value)
                await session.commit()
                await session.refresh(db_item)
            else:
                raise Exception(f"Record with ID {record_id} not found.")
    except SQLAlchemyError as e:
        error_message = f'Failed to update record with ID {record_id}: {str(e)}'
        raise Exception(error_message)

async def insert_record(BaseModel, new_row_data: dict) -> int:
    '''
    Insert a new celery record with the provided data.

    Args:
        new_row_data (dict): Data for the new celery record.

    Returns:
        int: ID of the newly inserted celery record.
    '''
    try:
        async with SessionLocal() as session:
            new_celery_item = BaseModel(**new_row_data)
            session.add(new_celery_item)
            await session.commit()
            await session.refresh(new_celery_item)
            return new_celery_item.project_id
    except SQLAlchemyError as e:
        error_message = f'Failed to insert a new celery record: {str(e)}'
        raise Exception(error_message)

async def delete_old_project(site_id: str, antena_height: float, record_id: int, site_type:str) -> None:
    '''
    Find the ID of a project by site_id and antenna_height.

    Args:
        site_id (str): The site ID to search for.
        antenna_height (int): The antenna height to search for.

    Returns:
        int: ID of the project if found, else None.
    '''
    try:
        async with SessionLocal() as session:
            result = await session.execute(
                select(pColopriming.project_id).where(pColopriming.site_id == site_id, pColopriming.antena_height == antena_height, pColopriming.project_id != record_id, pColopriming.site_type == site_type)
            )
            project_id = sorted(result.scalars().all())
            if len(project_id) > 0:
                for i in project_id:
                    await delete_record(pColopriming, i)
                    await delete_record(pdColopriming, i)
                    await delete_record(pdsColopriming, i)
                    await delete_record(spQuadranSec, i)
                    await delete_record(spBuildingSec, i)
                    await delete_record(spRoadSec, i)
                    await delete_record(spRoadCloseness, i)
                    await delete_record(spSectoralDistance, i)
                    await delete_record(spSectoralElevation, i)
    except SQLAlchemyError as e:
        error_message = f'Failed to find project ID: {str(e)}'
        raise Exception(error_message)


async def delete_old_siro_project(site_id: str, operator:str, record_id: int) -> None:
    try:
        async with SessionLocal() as session:
            result = await session.execute(
                select(pSiro.project_id).where(pSiro.site_id == site_id, pSiro.operator == operator, pSiro.project_id != record_id)
            )
            project_id = sorted(result.scalars().all())
            if len(project_id) > 0:
                for i in project_id:
                    await delete_record(pSiro, i)

    except SQLAlchemyError as e:
        error_message = f'Failed to find project ID: {str(e)}'
        raise Exception(error_message)


async def delete_current_project(record_id: int) -> None:
    '''
    Find the ID of a project by site_id and antenna_height.

    Args:
        site_id (str): The site ID to search for.
        antenna_height (int): The antenna height to search for.

    Returns:
        int: ID of the project if found, else None.
    '''
    try:
        await delete_record(pColopriming, record_id)
        await delete_record(pdColopriming, record_id)
        await delete_record(pdsColopriming, record_id)
        await delete_record(spQuadranSec, record_id)
        await delete_record(spBuildingSec, record_id)
        await delete_record(spRoadSec, record_id)
        await delete_record(spRoadCloseness, record_id)
        await delete_record(spSectoralDistance, record_id)
        await delete_record(spSectoralElevation, record_id)
    except SQLAlchemyError as e:
        error_message = f'Failed to find project ID: {str(e)}'
        raise Exception(error_message)


# Bulk Job Operations
async def create_bulk_job(job_data: dict) -> str:
    '''
    Create a new bulk job record.

    Args:
        job_data (dict): Data for the new bulk job.

    Returns:
        str: job_id of the newly created bulk job.
    '''
    try:
        async with SessionLocal() as session:
            new_job = pBulkJob(**job_data)
            session.add(new_job)
            await session.commit()
            await session.refresh(new_job)
            return new_job.job_id
    except SQLAlchemyError as e:
        error_message = f'Failed to create bulk job: {str(e)}'
        raise Exception(error_message)


async def update_bulk_job(job_id: str, updates: dict) -> None:
    '''
    Update a bulk job record.

    Args:
        job_id (str): ID of the bulk job to update.
        updates (dict): Dictionary of fields and their updated values.
    '''
    try:
        async with SessionLocal() as session:
            result = await session.execute(
                select(pBulkJob).where(pBulkJob.job_id == job_id)
            )
            job = result.scalars().first()
            if job:
                for field, value in updates.items():
                    if hasattr(job, field):
                        setattr(job, field, value)
                await session.commit()
                await session.refresh(job)
            else:
                raise Exception(f"Bulk job with ID {job_id} not found.")
    except SQLAlchemyError as e:
        error_message = f'Failed to update bulk job with ID {job_id}: {str(e)}'
        raise Exception(error_message)


async def get_bulk_job(job_id: str) -> dict:
    '''
    Get bulk job details by job_id.

    Args:
        job_id (str): ID of the bulk job.

    Returns:
        dict: Bulk job details.
    '''
    try:
        async with SessionLocal() as session:
            result = await session.execute(
                select(pBulkJob).where(pBulkJob.job_id == job_id)
            )
            job = result.scalars().first()
            if job:
                return {
                    'job_id': job.job_id,
                    'task_id': job.task_id,
                    'job_name': job.job_name,
                    'file_name': job.file_name,
                    'total_sites': job.total_sites,
                    'processed_sites': job.processed_sites,
                    'failed_sites': job.failed_sites,
                    'status': job.status,
                    'progress_percentage': job.progress_percentage,
                    'result_file_path': job.result_file_path,
                    'error_message': job.error_message,
                    'created_at': job.created_at,
                    'started_at': job.started_at,
                    'finished_at': job.finished_at,
                    'created_by': job.created_by
                }
            return None
    except SQLAlchemyError as e:
        error_message = f'Failed to get bulk job with ID {job_id}: {str(e)}'
        raise Exception(error_message)


async def create_bulk_job_detail(detail_data: dict) -> int:
    '''
    Create a bulk job detail record.

    Args:
        detail_data (dict): Data for the bulk job detail.

    Returns:
        int: ID of the newly created detail record.
    '''
    try:
        async with SessionLocal() as session:
            new_detail = pBulkJobDetail(**detail_data)
            session.add(new_detail)
            await session.commit()
            await session.refresh(new_detail)
            return new_detail.id
    except SQLAlchemyError as e:
        error_message = f'Failed to create bulk job detail: {str(e)}'
        raise Exception(error_message)


async def update_bulk_job_detail(detail_id: int, updates: dict) -> None:
    '''
    Update a bulk job detail record.

    Args:
        detail_id (int): ID of the bulk job detail to update.
        updates (dict): Dictionary of fields and their updated values.
    '''
    try:
        async with SessionLocal() as session:
            result = await session.execute(
                select(pBulkJobDetail).where(pBulkJobDetail.id == detail_id)
            )
            detail = result.scalars().first()
            if detail:
                for field, value in updates.items():
                    if hasattr(detail, field):
                        setattr(detail, field, value)
                await session.commit()
                await session.refresh(detail)
            else:
                raise Exception(f"Bulk job detail with ID {detail_id} not found.")
    except SQLAlchemyError as e:
        error_message = f'Failed to update bulk job detail with ID {detail_id}: {str(e)}'
        raise Exception(error_message)


async def get_bulk_job_details(job_id: str) -> list:
    '''
    Get all details for a bulk job.

    Args:
        job_id (str): ID of the bulk job.

    Returns:
        list: List of bulk job details.
    '''
    try:
        async with SessionLocal() as session:
            result = await session.execute(
                select(pBulkJobDetail).where(pBulkJobDetail.job_id == job_id).order_by(pBulkJobDetail.site_index)
            )
            details = result.scalars().all()
            return [
                {
                    'id': detail.id,
                    'job_id': detail.job_id,
                    'site_id': detail.site_id,
                    'site_index': detail.site_index,
                    'status': detail.status,
                    'project_id': detail.project_id,
                    'error_message': detail.error_message,
                    'processed_at': detail.processed_at
                } for detail in details
            ]
    except SQLAlchemyError as e:
        error_message = f'Failed to get bulk job details: {str(e)}'
        raise Exception(error_message)
