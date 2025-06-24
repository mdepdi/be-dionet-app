from celery import Celery
import os
from module import *
from celery.result import AsyncResult
import asyncio
from database import *
from modules.database_actions import *
import uuid
import csv
from io import StringIO
import traceback
import pandas as pd
from datetime import datetime

celery = Celery(__name__)
celery.conf.broker_url = os.environ.get('CELERY_BROKER_URL')
celery.conf.result_backend = os.environ.get('CELERY_RESULT_BACKEND')
celery.conf.task_serializer = 'json'

async def async_colopriming_analysis(colopriming_site, record_id, task_id, task):
    await asyncio.sleep(1)  # Properly await sleep
    update = {'task_id': task_id, 'status': "STARTED",'created_at' : datetime.now()}
    await update_record(pColopriming,record_id, update)
    result = await colopriming_analysis(colopriming_site, record_id, task_id, task, 'background', False)
    return result

@celery.task
def celery_colopriming_anaysis(colopriming_site, record_id):
    task_id = celery_colopriming_anaysis.request.id
    task = AsyncResult(task_id)
    # Use the default event loop
    loop = asyncio.get_event_loop()
    # Run the async function in the event loop
    result = loop.run_until_complete(async_colopriming_analysis(colopriming_site, record_id, task_id, task))
    return result

async def async_colopriming_analysis_siro(colopriming_site, record_id, task_id, task):
    await asyncio.sleep(1)  # Properly await sleep
    update = {'task_id': task_id, 'status': "STARTED",'created_at' : datetime.now()}
    await update_record(pSiro,record_id, update)
    result = await colopriming_siro_analysis(colopriming_site, record_id, task_id, task, 'background', True)
    return result

@celery.task
def celery_colopriming_anaysis_siro(colopriming_site, record_id):
    task_id = celery_colopriming_anaysis_siro.request.id
    task = AsyncResult(task_id)
    # Use the default event loop
    loop = asyncio.get_event_loop()
    # Run the async function in the event loop
    result = loop.run_until_complete(async_colopriming_analysis_siro(colopriming_site, record_id, task_id, task))
    return result


async def async_bulk_colopriming_analysis(job_id, sites_data, output_filename):
    """
    Process bulk colopriming analysis asynchronously
    """
    try:
        # Update job status to PROCESSING
        await update_bulk_job(job_id, {
            'status': 'PROCESSING',
            'started_at': datetime.now()
        })

        results = []
        total_sites = len(sites_data)
        processed_count = 0
        failed_count = 0

        # Get bulk job details to update individual site statuses
        job_details = await get_bulk_job_details(job_id)
        detail_lookup = {detail['site_index']: detail['id'] for detail in job_details}

        for idx, site_data in enumerate(sites_data):
            try:
                detail_id = detail_lookup.get(idx)

                # Update detail status to PROCESSING
                if detail_id:
                    await update_bulk_job_detail(detail_id, {
                        'status': 'PROCESSING'
                    })

                # Process the site
                result = await bulk_colopriming_analysis(site_data)

                # Add site data to result - handle ALL operators like regular bulk
                if result and 'result' in result and len(result['result']) > 0:
                    for i, operator_result in enumerate(result['result']):
                        site_result = site_data.copy()
                        for k, v in operator_result.items():
                            site_result[k] = v
                        results.append(site_result)
                else:
                    # If no results, still add the original site data
                    results.append(site_data.copy())

                processed_count += 1

                # Update detail status to SUCCESS
                if detail_id:
                    await update_bulk_job_detail(detail_id, {
                        'status': 'SUCCESS',
                        'processed_at': datetime.now()
                    })

            except Exception as site_error:
                failed_count += 1
                error_msg = str(site_error)

                # Add failed site to results with error
                site_result = site_data.copy()
                site_result['error'] = error_msg
                results.append(site_result)

                # Update detail status to FAILURE
                if detail_id:
                    await update_bulk_job_detail(detail_id, {
                        'status': 'FAILURE',
                        'error_message': error_msg,
                        'processed_at': datetime.now()
                    })

            # Update job progress
            progress = ((processed_count + failed_count) / total_sites) * 100
            await update_bulk_job(job_id, {
                'processed_sites': processed_count,
                'failed_sites': failed_count,
                'progress_percentage': progress
            })

        # Save results to CSV
        result_df = pd.DataFrame(results)

        # Ensure the output directory exists
        output_dir = './dionet_data'
        os.makedirs(output_dir, exist_ok=True)

        output_path = f'{output_dir}/{output_filename}.csv'
        result_df.to_csv(output_path, index=False)

        # Update job status to SUCCESS
        await update_bulk_job(job_id, {
            'status': 'SUCCESS',
            'finished_at': datetime.now(),
            'result_file_path': output_path,
            'processed_sites': processed_count,
            'failed_sites': failed_count,
            'progress_percentage': 100.0
        })

        return {
            'job_id': job_id,
            'status': 'SUCCESS',
            'total_sites': total_sites,
            'processed_sites': processed_count,
            'failed_sites': failed_count,
            'result_file_path': output_path
        }

    except Exception as e:
        error_msg = f"Bulk processing failed: {str(e)}"
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        full_error = f"{error_msg}\n{tb_str}"

        # Update job status to FAILURE
        await update_bulk_job(job_id, {
            'status': 'FAILURE',
            'finished_at': datetime.now(),
            'error_message': full_error
        })

        raise Exception(full_error)


@celery.task
def celery_bulk_colopriming_analysis(job_id, sites_data, output_filename):
    """
    Celery task for bulk colopriming analysis
    """
    task_id = celery_bulk_colopriming_analysis.request.id

    # Update job with task_id
    loop = asyncio.get_event_loop()
    loop.run_until_complete(update_bulk_job(job_id, {'task_id': task_id}))

    # Run the async bulk analysis
    result = loop.run_until_complete(async_bulk_colopriming_analysis(job_id, sites_data, output_filename))
    return result