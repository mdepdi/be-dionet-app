from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi import FastAPI, Security, Response, Depends, Request,File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from fastapi.responses import FileResponse, StreamingResponse

import csv
from io import StringIO
import io

from dotenv import load_dotenv
from sqlalchemy import inspect,text
from sqlalchemy.orm import Session

import os
import logging
import uuid
import traceback
from datetime import datetime
from pydantic import BaseModel, ValidationError
from typing import List, Dict, Optional

import asyncio
from sse_starlette.sse import EventSourceResponse
from fastapi import HTTPException
from sqlalchemy import select

from authentication import api_key_header, get_api_key, get_api_key_admin
from populate_database import *
from module import *
from modules.database_actions import *
from database import *
import json

from celery_worker import (
    celery_colopriming_analysis,
    celery_colopriming_analysis_siro,
    celery_bulk_colopriming_analysis
)

import shutil
from pathlib import Path

app = FastAPI(docs_url=None, redoc_url=None, title="Radio Network Spatial API", version="1.0.0", swagger_ui_parameters={"operationsSorter":"alpha"})

load_dotenv()
api_keys = [os.environ.get('API_KEY')]
base_url = os.environ.get('BASE_URL')
api_key_header = APIKeyHeader(name='X-API-Key')

origins = [
    "http://localhost",
    "http://localhost:8000",
    "http://localhost:3000",
    "http://127.0.0.1:8000",
    "http://127.0.0.1:3000",
    "http://20.2.2.28:3000",
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


logging.basicConfig(level=logging.INFO)

@app.get("/")
async def root():
    return {"message": "BE DIONET IS RUNNING"}

@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title + " - Swagger UI",
        oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
        swagger_js_url="https://unpkg.com/swagger-ui-dist@5.9.0/swagger-ui-bundle.js",
        swagger_css_url="https://unpkg.com/swagger-ui-dist@5.9.0/swagger-ui.css",
    )


@app.get(app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
async def swagger_ui_redirect():
    return get_swagger_ui_oauth2_redirect_html()


@app.get("/redoc", include_in_schema=False)
async def redoc_html():
    return get_redoc_html(
        openapi_url=app.openapi_url,
        title=app.title + " - ReDoc",
        redoc_js_url="https://unpkg.com/redoc@next/bundles/redoc.standalone.js",
    )


@app.get("/migrateDatabase")
def handle_migrate_database(response: Response):
    return migrate_database(response)

@app.get("/populateDatabase/{table}/tables")
def handle_populate_database(table: tables, response: Response):
    return populate_database(response, table)

@app.get("/flushDatabase/{table}/tables")
def handle_flush_tables(table: tables, response: Response, api_key: str = Security(get_api_key_admin)):
    return flush_tables(response, table)


class ColoprimingSite(BaseModel):
    site_id:str
    longitude: float
    latitude: float
    tower_height: float
    antena_height: float
    operator: List[str]
    site_type: str

@app.post('/doColopriming', status_code=201)
async def handle_do_colopriming(colopriming_site: ColoprimingSite) -> Dict:
    try:
        project = {
                'site_id': colopriming_site.site_id,
                'status': "RECEIVED",
                'type': 'direct',
                'antena_height': colopriming_site.antena_height,
                'tower_height': colopriming_site.tower_height,
                'longitude': colopriming_site.longitude,
                'latitude': colopriming_site.latitude,
                'site_type': colopriming_site.site_type,
                'created_at': datetime.now(),
            }

        record_id = await insert_record(pColopriming, project)

        return await colopriming_analysis(colopriming_site, record_id, None, None, running_type='direct', isSiro=False)

    except ValidationError as e:
        return {'Validation Error': e}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Run Task error {e}')

@app.post('/doColoprimingSiro', status_code=201)
async def handle_do_colopriming_siro(colopriming_site: ColoprimingSite) -> Dict:
    try:
        project = {
                'site_id': colopriming_site.site_id,
                'status': "RECEIVED",
                'type': 'direct',
                'operator': colopriming_site.operator[0],
                'created_at': datetime.now(),
            }

        record_id = await insert_record(pSiro, project)
        # print(f"==>> record_id: {record_id}")
        return await colopriming_siro_analysis(colopriming_site, record_id,None, None,  running_type='direct', isSiro=True)

    except ValidationError as e:
        return {'Validation Error': e}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Run Task error {e}')

@app.post('/celeryColoprimingSiro', status_code=201)
async def handle_celery_colopriming_siro(colopriming_site: ColoprimingSite) -> Dict:
    try:
        project = {
            'site_id': colopriming_site.site_id,
            'type': 'background',
            'operator': colopriming_site.operator[0],
            'status': "RECEIVED",
            'created_at': datetime.now()
        }

        record_id = await insert_record(pSiro, project)

        celery_task = celery_colopriming_analysis_siro.apply_async(
            args=[colopriming_site.model_dump(), record_id],
            queue='dionet_queue',
            routing_key='dionet.siro'
        )

        return {
            'task_id': celery_task.id,
            'project_id': record_id,
            'operator': colopriming_site.operator[0],
            'site_id': colopriming_site.site_id,
            'type': 'background',
            'created_at': datetime.now(),
        }

    except ValidationError as e:
        return {'Validation Error': e}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Run Task error {e}')


@app.post('/celeryColopriming', status_code=201)
async def handle_celery_colopriming(colopriming_site: ColoprimingSite) -> Dict:
    try:
        project = {
            'site_id': colopriming_site.site_id,
            'type': 'background',
            'status': "RECEIVED",
            'antena_height': colopriming_site.antena_height,
            'tower_height': colopriming_site.tower_height,
            'longitude': colopriming_site.longitude,
            'latitude': colopriming_site.latitude,
            'site_type': colopriming_site.site_type,
            'created_at': datetime.now()
        }

        record_id = await insert_record(pColopriming, project)

        celery_task = celery_colopriming_analysis.apply_async(
            args=[colopriming_site.model_dump(), record_id],
            queue='dionet_queue',
            routing_key='dionet.colopriming'
        )

        return {
            'task_id': celery_task.id,
            'project_id': record_id,
            'site_id': colopriming_site.site_id,
            'type': 'background',
            'antena_height': colopriming_site.antena_height,
            'tower_height': colopriming_site.tower_height,
            'longitude': colopriming_site.longitude,
            'latitude': colopriming_site.latitude,
            'site_type': colopriming_site.site_type,
            'created_at': datetime.now(),
        }

    except ValidationError as e:
        return {'Validation Error': e}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Run Task error {e}')

STREAM_DELAY = 1  # second
RETRY_TIMEOUT = 2000  # milisecond

@app.get('/stream')
async def message_stream(request: Request, site_id: str, antena_height: float):
    async def new_messages(site_id: str, antena_height: float):
        query = text(f"""
            SELECT colo.site_id, colo.antena_height, colo.status
            FROM
                p_colopriming AS colo
            WHERE
                colo.site_id = '{site_id}'
                AND colo.antena_height ={antena_height}
        """)
        async with SessionLocal() as session:
            status = await session.execute(query)
            status = status.fetchall()
            status = pd.DataFrame(status)
            if not status.empty:
                return status['status'].iloc[0]
            return "NO_STATUS"

    async def event_generator():
        print("Client connected.")
        try:
            while True:
                # If client closes connection, stop sending events
                if await request.is_disconnected():
                    break

                message = await new_messages(site_id, antena_height)
                event_data = json.dumps({
                    "event": "new_message",
                    "id": "message_id",
                    "retry": RETRY_TIMEOUT,
                    "data": message
                })

                # Check for new messages and return them to client if any
                if message in ['SUCCESS', 'FAILURE']:
                    yield f"data: {event_data}\n\n"
                    print("Event streaming stopped.")
                    break  # Stop streaming on SUCCESS or FAILURE

                yield f"data: {event_data}\n\n"
                await asyncio.sleep(STREAM_DELAY)

        except asyncio.CancelledError:
            print("Event streaming stopped.")
    response = EventSourceResponse(event_generator())
    print(f"==>> response: {response}")

    return response


@app.get('/stream_siro')
async def message_stream(request: Request, site_id: str, operator: str):
    async def new_messages(site_id: str, operator: str):
        query = text(f"""
            SELECT siro.site_id, siro.operator, siro.status
            FROM
                p_siro AS siro
            WHERE
                siro.site_id = '{site_id}'
                AND siro.operator = '{operator}'
        """)
        async with SessionLocal() as session:
            status = await session.execute(query)
            status = status.fetchall()
            status = pd.DataFrame(status)
            if not status.empty:
                return status['status'].iloc[0]
            return "NO_STATUS"

    async def event_generator():
        print("Client connected.")
        try:
            while True:
                # If client closes connection, stop sending events
                if await request.is_disconnected():
                    break

                message = await new_messages(site_id, operator)
                event_data = json.dumps({
                    "event": "new_message",
                    "id": "message_id",
                    "retry": RETRY_TIMEOUT,
                    "data": message
                })

                # Check for new messages and return them to client if any
                if message in ['SUCCESS', 'FAILURE']:
                    yield f"data: {event_data}\n\n"
                    print("Event streaming stopped.")
                    break  # Stop streaming on SUCCESS or FAILURE

                yield f"data: {event_data}\n\n"
                await asyncio.sleep(STREAM_DELAY)

        except asyncio.CancelledError:
            print("Event streaming stopped.")
    response = EventSourceResponse(event_generator())
    print(f"==>> response: {response}")

    return response



@app.post("/bulk")
async def upload(file: UploadFile = File(...), output_file_name: str = Form(...)):
    sites = []
    results = []
    try:
        file_bytes = await file.read()
        buffer = StringIO(file_bytes.decode('utf-8'))

        # Process CSV
        csvReader = csv.DictReader(buffer)
        for row in csvReader:
            data = {}
            for key in row:
                if key in ["site_type", "site_id", "operator"]:
                    data[key] = str(row[key])
                else:
                    try:
                        data[key] = float(row[key])  # Convert numerical fields
                    except ValueError:
                        data[key] = row[key]  # Keep as string if conversion fails
            sites.append(data)

        pd_result = pd.DataFrame()


        # Process sites in batches
        for idx, colopriming_site in tqdm(enumerate(sites, start=1), total=len(sites)):
            try:
                single = await bulk_colopriming_analysis(colopriming_site)

                for i, j in enumerate(single['result']):
                    for k in j:
                        colopriming_site[k] = single['result'][i][k]

                    pd_df = pd.DataFrame([colopriming_site])
                    pd_result = pd.concat([pd_result, pd_df], ignore_index=True)

            except Exception as e:
                tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
                print(f'ERROR: {e}\n{tb_str}')
                continue  # Skip to the next row

            # Save output every 50 rows
            if idx % 50 == 0 or idx == len(sites):
                # Ensure the step directory exists
                step_dir = "./dionet_data/step"
                os.makedirs(step_dir, exist_ok=True)
                output_filename = f"{step_dir}/{output_file_name}_{idx}.csv"
                pd_result.to_csv(output_filename, index=False)
                print(f"==>> Saved: {output_filename}")


        # Convert DataFrame to CSV in memory
        # Ensure the output directory exists
        output_dir = './dionet_data'
        os.makedirs(output_dir, exist_ok=True)

        final_output_path = f'{output_dir}/{output_file_name}.csv'
        pd_result.to_csv(final_output_path, index=False)
        csv_buffer = StringIO()
        pd_result.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)  # Reset pointer to the beginning

        buffer.close()
        file.file.close()

        # # Return CSV as StreamingResponse
        return FileResponse(
            final_output_path,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={output_file_name}.csv"}
        )

        # return {"message": "Processing completed", "total_processed": len(sites)}

    except Exception as e:
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {e}\n{tb_str}")

async def process_site(colopriming_site: Dict) -> Dict:
    try:
        single = await bulk_colopriming_analysis(colopriming_site)
        for k, v in single['result'][0].items():
            colopriming_site[k] = v
        return colopriming_site
    except Exception as e:
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        print(f'ERROR: {e}\n{tb_str}')
        raise HTTPException(status_code=500, detail=f'ERROR: {e}\n{tb_str}')




@app.post('/devColopriming', status_code=201)
async def handle_dev_colopriming(colopriming_site: ColoprimingSite) -> Dict:
    return await colopriming_analysis(colopriming_site, 99999, None, None, 'dev',False)

# New Bulk Processing Endpoints

class BulkJobResponse(BaseModel):
    job_id: str
    task_id: Optional[str]
    job_name: str
    file_name: str
    total_sites: int
    status: str
    created_at: datetime
    created_by: Optional[str]


@app.post('/celeryBulk', status_code=201)
async def handle_celery_bulk_upload(
    file: UploadFile = File(...),
    job_name: str = Form(...),
    api_key: str = Security(get_api_key)
) -> BulkJobResponse:
    """
    Upload CSV file for bulk colopriming analysis using Celery background processing
    """
    try:
        # Generate unique job ID
        job_id = str(uuid.uuid4())

        # Read and parse CSV file
        file_bytes = await file.read()
        buffer = StringIO(file_bytes.decode('utf-8'))

        sites = []
        csvReader = csv.DictReader(buffer)

        for row_idx, row in enumerate(csvReader):
            data = {}
            for key in row:
                if key in ["site_type", "site_id", "operator"]:
                    data[key] = str(row[key]) if row[key] is not None else ""
                else:
                    try:
                        # Check if value is None, empty string, or whitespace only
                        if row[key] is None or str(row[key]).strip() == "":
                            data[key] = None
                        else:
                            data[key] = float(row[key])  # Convert numerical fields
                    except (ValueError, TypeError):
                        # Keep as original value if conversion fails
                        data[key] = row[key] if row[key] is not None else None
            sites.append(data)

        buffer.close()
        file.file.close()

        if not sites:
            raise HTTPException(status_code=400, detail="No valid sites found in the uploaded file")

        # Create bulk job record
        job_data = {
            'job_id': job_id,
            'job_name': job_name,
            'file_name': file.filename,
            'total_sites': len(sites),
            'status': 'PENDING',
            'created_at': datetime.now(),
            'created_by': api_key  # Store API key as creator identifier
        }

        await create_bulk_job(job_data)

        # Create detail records for each site
        for idx, site_data in enumerate(sites):
            detail_data = {
                'job_id': job_id,
                'site_id': site_data.get('site_id', f'site_{idx}'),
                'site_index': idx,
                'longitude': site_data.get('longitude'),
                'latitude': site_data.get('latitude'),
                'tower_height': site_data.get('tower_height'),
                'antena_height': site_data.get('antena_height'),
                'site_type': site_data.get('site_type'),
                'operator': site_data.get('operator', ''),
                'status': 'PENDING'
            }
            await create_bulk_job_detail(detail_data)

        # Start Celery task
        output_filename = f"bulk_result_{job_id}"
        celery_task = celery_bulk_colopriming_analysis.apply_async(
            args=[job_id, sites, output_filename],
            queue='dionet_queue',
            routing_key='dionet.bulk'
        )

        # Update job with task ID
        await update_bulk_job(job_id, {'task_id': celery_task.id})

        return BulkJobResponse(
            job_id=job_id,
            task_id=celery_task.id,
            job_name=job_name,
            file_name=file.filename,
            total_sites=len(sites),
            status='PENDING',
            created_at=datetime.now(),
            created_by=api_key
        )

    except Exception as e:
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        raise HTTPException(status_code=500, detail=f"Bulk upload failed: {str(e)}\n{tb_str}")


@app.get('/bulkStatus/{job_id}')
async def get_bulk_job_status(job_id: str, api_key: str = Security(get_api_key)):
    """
    Get the status of a bulk processing job
    """
    try:
        job = await get_bulk_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Bulk job not found")

        # Get job details for more information
        details = await get_bulk_job_details(job_id)

        return {
            **job,
            'details_count': len(details),
            'success_count': len([d for d in details if d['status'] == 'SUCCESS']),
            'processing_count': len([d for d in details if d['status'] == 'PROCESSING']),
            'pending_count': len([d for d in details if d['status'] == 'PENDING']),
            'failed_count': len([d for d in details if d['status'] == 'FAILURE'])
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get job status: {str(e)}")


@app.get('/bulkStream/{job_id}')
async def bulk_progress_stream(request: Request, job_id: str, api_key: str = Security(get_api_key)):
    """
    Server-Sent Events stream for real-time bulk processing progress
    """
    async def get_job_progress(job_id: str):
        """Get current job progress"""
        try:
            job = await get_bulk_job(job_id)
            if not job:
                return None
            return {
                'job_id': job_id,
                'status': job['status'],
                'progress_percentage': job['progress_percentage'],
                'processed_sites': job['processed_sites'],
                'failed_sites': job['failed_sites'],
                'total_sites': job['total_sites'],
                'started_at': job['started_at'].isoformat() if job['started_at'] else None,
                'finished_at': job['finished_at'].isoformat() if job['finished_at'] else None,
                'error_message': job['error_message']
            }
        except Exception as e:
            return {'error': str(e)}

    async def event_generator():
        print(f"Client connected to bulk stream for job: {job_id}")
        try:
            while True:
                # Check if client disconnected
                if await request.is_disconnected():
                    break

                progress = await get_job_progress(job_id)

                if progress is None:
                    event_data = json.dumps({
                        "event": "error",
                        "data": "Job not found"
                    })
                    yield f"data: {event_data}\n\n"
                    break

                event_data = json.dumps({
                    "event": "progress_update",
                    "id": f"progress_{datetime.now().timestamp()}",
                    "retry": 2000,
                    "data": progress
                })

                yield f"data: {event_data}\n\n"

                # Stop streaming if job is completed
                if progress.get('status') in ['SUCCESS', 'FAILURE']:
                    print(f"Bulk job {job_id} completed with status: {progress.get('status')}")
                    break

                await asyncio.sleep(2)  # Update every 2 seconds

        except asyncio.CancelledError:
            print(f"Bulk streaming stopped for job: {job_id}")
        except Exception as e:
            error_event = json.dumps({
                "event": "error",
                "data": str(e)
            })
            yield f"data: {error_event}\n\n"

    return EventSourceResponse(event_generator())


@app.get('/bulkDownload/{job_id}')
async def download_bulk_results(job_id: str, api_key: str = Security(get_api_key)):
    """
    Download the results file from a completed bulk processing job
    """
    try:
        job = await get_bulk_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Bulk job not found")

        if job['status'] != 'SUCCESS':
            raise HTTPException(status_code=400, detail=f"Job is not completed successfully. Current status: {job['status']}")

        if not job['result_file_path']:
            raise HTTPException(status_code=404, detail="Result file path not set")

        if not os.path.exists(job['result_file_path']):
            # Additional debugging info
            print(f"DEBUG: Looking for file at: {job['result_file_path']}")
            print(f"DEBUG: File exists check failed. Directory listing:")
            try:
                dir_path = os.path.dirname(job['result_file_path'])
                print(f"DEBUG: Directory {dir_path} contents: {os.listdir(dir_path) if os.path.exists(dir_path) else 'Directory does not exist'}")
            except Exception as debug_e:
                print(f"DEBUG: Error listing directory: {debug_e}")
            raise HTTPException(status_code=404, detail=f"Result file not found at path: {job['result_file_path']}")

        filename = f"bulk_results_{job['job_name']}_{job_id}.csv"

        return FileResponse(
            job['result_file_path'],
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Failed to download results: {str(e)}")


@app.get('/bulkJobs')
async def list_bulk_jobs(api_key: str = Security(get_api_key), limit: int = 50, offset: int = 0):
    """
    List bulk processing jobs (with pagination)
    """
    try:
        async with SessionLocal() as session:
            # Get jobs with pagination
            result = await session.execute(
                select(pBulkJob)
                .where(pBulkJob.active == True)
                .order_by(pBulkJob.created_at.desc())
                .limit(limit)
                .offset(offset)
            )
            jobs = result.scalars().all()

            job_list = []
            for job in jobs:
                job_list.append({
                    'job_id': job.job_id,
                    'job_name': job.job_name,
                    'file_name': job.file_name,
                    'total_sites': job.total_sites,
                    'processed_sites': job.processed_sites,
                    'failed_sites': job.failed_sites,
                    'status': job.status,
                    'progress_percentage': job.progress_percentage,
                    'created_at': job.created_at,
                    'started_at': job.started_at,
                    'finished_at': job.finished_at,
                    'created_by': job.created_by
                })

            return {
                'jobs': job_list,
                'total_count': len(job_list),
                'limit': limit,
                'offset': offset
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list jobs: {str(e)}")


@app.delete('/bulkJob/{job_id}')
async def delete_bulk_job(job_id: str, api_key: str = Security(get_api_key_admin)):
    """
    Delete a bulk job and its associated data (Admin only)
    """
    try:
        job = await get_bulk_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Bulk job not found")

        # Mark job as inactive instead of deleting
        await update_bulk_job(job_id, {'active': False})

        # Also mark all details as inactive
        async with SessionLocal() as session:
            await session.execute(
                text("UPDATE p_bulk_job_detail SET active = false WHERE job_id = :job_id"),
                {"job_id": job_id}
            )
            await session.commit()

        # Clean up result file if exists
        if job['result_file_path'] and os.path.exists(job['result_file_path']):
            try:
                os.remove(job['result_file_path'])
            except Exception as file_error:
                print(f"Warning: Could not delete result file: {file_error}")

        return {"message": f"Bulk job {job_id} deleted successfully"}

    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Failed to delete job: {str(e)}")

class DataUploadResponse(BaseModel):
    success: bool
    message: str
    table_name: str
    file_size: int
    records_imported: Optional[int] = None

@app.post('/uploadDataFile/{table_name}', status_code=201)
async def upload_data_file(
    table_name: str,
    file: UploadFile = File(...),
    overwrite: bool = Form(True),
    populate_immediately: bool = Form(True),
    flush_existing: bool = Form(False),
    api_key: str = Security(get_api_key_admin)
) -> DataUploadResponse:
    """
    Upload data files for specific tables and optionally populate database
    Supported tables: m_colopriming, m_operator, m_colopriming_sopt
    Supported formats: .csv, .parquet
    """
    try:
        # Validate table name
        supported_tables = ['m_colopriming', 'm_operator', 'm_colopriming_sopt', 'm_siro']
        if table_name not in supported_tables:
            raise HTTPException(
                status_code=400,
                detail=f"Table '{table_name}' not supported. Supported tables: {supported_tables}"
            )

        # Validate file format
        file_extension = Path(file.filename).suffix.lower()
        if file_extension not in ['.csv', '.parquet']:
            raise HTTPException(
                status_code=400,
                detail="Only .csv and .parquet files are supported"
            )

        # Determine target filename based on table
        filename_mapping = {
            'm_colopriming': 'tb_colopriming.parquet' if file_extension == '.parquet' else 'tb_colopriming.csv',
            'm_operator': 'tb_operator.parquet' if file_extension == '.parquet' else 'tb_operator.csv',
            'm_colopriming_sopt': 'tb_colopriming_sopt.parquet' if file_extension == '.parquet' else 'tb_colopriming_sopt.csv',
            'm_siro': 'siro.parquet' if file_extension == '.parquet' else 'siro.csv'
        }

        target_filename = filename_mapping[table_name]
        target_path = f"./data/{target_filename}"

        # Check if file exists and overwrite is not allowed
        if os.path.exists(target_path) and not overwrite:
            raise HTTPException(
                status_code=409,
                detail=f"File {target_filename} already exists. Set overwrite=true to replace it."
            )

        # Save uploaded file
        file_content = await file.read()
        file_size = len(file_content)

        # Backup existing file if it exists
        if os.path.exists(target_path) and overwrite:
            backup_path = f"{target_path}.backup"
            shutil.copy2(target_path, backup_path)
            logger.info(f"Backed up existing file to {backup_path}")

        # Write new file
        with open(target_path, 'wb') as f:
            f.write(file_content)

        logger.info(f"Successfully uploaded {file.filename} to {target_path}")

        records_imported = None

                # Populate database if requested
        if populate_immediately:
            try:
                # Create a response object for populate_database function
                from fastapi.responses import JSONResponse
                temp_response = Response()

                # Flush existing data if requested
                if flush_existing:
                    logger.info(f"Flushing existing data for table {table_name}")
                    flush_result = flush_tables(temp_response, table_name)
                    logger.info(f"Successfully flushed existing data for {table_name}")

                # Call populate_database function
                result = populate_database(temp_response, table_name)

                # Count records (optional - requires additional query)
                db = SessionSync()
                try:
                    model = tablesModel[table_name]['model']
                    records_imported = db.query(model).filter_by(active=True).count()
                except:
                    records_imported = None
                finally:
                    db.close()

                logger.info(f"Successfully populated {table_name} with {records_imported} records")

            except Exception as populate_error:
                # If populate fails, still return success for file upload
                logger.error(f"File uploaded successfully but populate failed: {populate_error}")
                return DataUploadResponse(
                    success=True,
                    message=f"File uploaded successfully but database population failed: {str(populate_error)}",
                    table_name=table_name,
                    file_size=file_size,
                    records_imported=None
                )

        # Create detailed success message
        message_parts = ["File uploaded successfully"]
        if populate_immediately:
            if flush_existing:
                message_parts.append("existing data flushed")
            message_parts.append("database populated")

        success_message = " and ".join(message_parts)

        return DataUploadResponse(
            success=True,
            message=success_message,
            table_name=table_name,
            file_size=file_size,
            records_imported=records_imported
        )

    except Exception as e:
        logger.error(f"Upload failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.get('/dataFileInfo/{table_name}')
async def get_data_file_info(
    table_name: str,
    request: Request,
    api_key: str = Security(get_api_key)
):
    """
    Get information about data files for specific tables
    """
    try:

        supported_tables = ['m_colopriming', 'm_operator', 'm_colopriming_sopt', 'm_siro']
        if table_name not in supported_tables:
            raise HTTPException(
                status_code=400,
                detail=f"Table '{table_name}' not supported. Supported tables: {supported_tables}"
            )

        filename_mapping = {
            'm_colopriming': ['tb_colopriming.parquet', 'tb_colopriming.csv'],
            'm_operator': ['tb_operator.parquet', 'tb_operator.csv'],
            'm_colopriming_sopt': ['tb_colopriming_sopt.parquet', 'tb_colopriming_sopt.csv'],
            'm_siro': ['siro.parquet', 'siro.csv']
        }

        files_info = []
        for filename in filename_mapping[table_name]:
            file_path = f"./data/{filename}"
            if os.path.exists(file_path):
                stat_info = os.stat(file_path)
                files_info.append({
                    'filename': filename,
                    'size': stat_info.st_size,
                    'last_modified': datetime.fromtimestamp(stat_info.st_mtime).isoformat(),
                    'exists': True
                })
            else:
                files_info.append({
                    'filename': filename,
                    'exists': False
                })

                        # Get database record count
        db = SessionSync()
        try:
            if table_name in tablesModel:
                model = tablesModel[table_name]['model']
                                # Add explicit commit to ensure fresh data
                db.commit()

                total_count = db.query(model).count()
                active_count = db.query(model).filter_by(active=True).count()

                record_count = active_count
            else:
                record_count = 0
        except Exception as e:
            record_count = 0
        finally:
            db.close()

        return {
            'table_name': table_name,
            'files': files_info,
            'database_records': record_count
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get file info: {str(e)}")