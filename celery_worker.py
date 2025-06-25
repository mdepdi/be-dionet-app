from celery import Celery
import os
from celery.result import AsyncResult
import asyncio
from database import *
from modules.database_actions import *
from module import (
    colopriming_analysis,
    colopriming_siro_analysis,
    bulk_colopriming_analysis
)
import uuid
import csv
from io import StringIO
import traceback
import pandas as pd
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

celery = Celery(__name__)
celery.conf.broker_url = os.environ.get('CELERY_BROKER_URL')
celery.conf.result_backend = os.environ.get('CELERY_RESULT_BACKEND')
celery.conf.task_serializer = 'json'
celery.conf.accept_content = ['json']
celery.conf.result_serializer = 'json'
celery.conf.timezone = 'UTC'
celery.conf.enable_utc = True

# Add task tracking
celery.conf.task_track_started = True
celery.conf.task_send_sent_event = True

# Set task time limits - increased for build-to-suit processing
celery.conf.task_time_limit = int(os.environ.get('CELERY_TASK_TIME_LIMIT', 3600))  # 1 hour hard limit
celery.conf.task_soft_time_limit = int(os.environ.get('CELERY_TASK_SOFT_TIME_LIMIT', 3300))  # 55 minutes soft limit

# Worker settings
celery.conf.worker_prefetch_multiplier = int(os.environ.get('CELERY_WORKER_PREFETCH_MULTIPLIER', 1))
celery.conf.worker_max_tasks_per_child = 1000
celery.conf.worker_concurrency = int(os.environ.get('CELERY_WORKER_CONCURRENCY', 4))

# Improved task handling settings
celery.conf.task_ignore_result = False
celery.conf.task_store_errors_even_if_ignored = True
celery.conf.task_reject_on_worker_lost = True
celery.conf.worker_disable_rate_limits = True
celery.conf.task_always_eager = False
celery.conf.task_eager_propagates = True

# Only accept registered tasks
celery.conf.task_routes = {
    'celery_colopriming_analysis': {'queue': 'celery'},
    'celery_colopriming_analysis_siro': {'queue': 'celery'},
    'celery_bulk_colopriming_analysis': {'queue': 'celery'},
}

def get_or_create_event_loop():
    """
    Safely get or create an event loop for async operations
    This prevents issues with event loop reuse in celery tasks
    """
    try:
        # Try to get the current event loop
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No event loop running, create a new one
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError("Event loop is closed")
        except RuntimeError:
            # Create new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    return loop

def log_build_to_suit_task(colopriming_site, task_id, status, message=""):
    """Helper function to log build-to-suit specific information"""
    site_type = colopriming_site.get('site_type', 'unknown')
    site_id = colopriming_site.get('site_id', 'unknown')

    log_msg = f"🏗️  BUILD-TO-SUIT [{status}] Task {task_id[:8]}... | Site: {site_id} | Type: {site_type}"
    if message:
        log_msg += f" | {message}"

    if status == "ERROR":
        logger.error(log_msg)
    elif status == "WARNING":
        logger.warning(log_msg)
    else:
        logger.info(log_msg)

async def async_colopriming_analysis(colopriming_site, record_id, task_id, task):
    await asyncio.sleep(1)  # Properly await sleep

    # Special handling for build-to-suit
    if colopriming_site.get('site_type') == 'build-to-suit':
        log_build_to_suit_task(colopriming_site, task_id, "PROCESSING", "Starting build-to-suit analysis")

    update = {'task_id': task_id, 'status': "STARTED",'created_at' : datetime.now()}
    await update_record(pColopriming,record_id, update)
    result = await colopriming_analysis(colopriming_site, record_id, task_id, task, 'background', False)
    return result

@celery.task(bind=True, name='celery_colopriming_analysis',
              autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 60})
def celery_colopriming_analysis(self, colopriming_site, record_id):
    try:
        task_id = self.request.id
        task = AsyncResult(task_id)
        retries = self.request.retries

        # Enhanced logging for build-to-suit tasks
        site_type = colopriming_site.get('site_type', 'unknown')
        if site_type == 'build-to-suit':
            log_build_to_suit_task(colopriming_site, task_id, "STARTED", f"Attempt {retries + 1}/4")

        logger.info(f"🚀 Starting colopriming analysis task: {task_id} for record: {record_id} (attempt {retries + 1}/4) | Site Type: {site_type}")

        # Immediately update status to show task is being processed
        self.update_state(
            state='PROCESSING',
            meta={'current': 0, 'total': 100, 'status': f'Task received and processing {site_type}...'}
        )

        # Get or create event loop safely
        loop = get_or_create_event_loop()

        # Update task status to STARTED immediately
        try:
            loop.run_until_complete(update_record(pColopriming, record_id, {
                'status': 'STARTED',
                'task_id': task_id
            }))
        except Exception as update_error:
            logger.warning(f"⚠️  Warning: Could not update task status: {update_error}")

        # Run the async function in the event loop
        result = loop.run_until_complete(async_colopriming_analysis(colopriming_site, record_id, task_id, task))
        logger.info(f"✅ Colopriming analysis completed for record: {record_id}")
        return result

    except Exception as e:
        site_type = colopriming_site.get('site_type', 'unknown')
        error_msg = f"❌ Colopriming analysis task failed ({site_type}): {str(e)}"

        # Enhanced error logging for build-to-suit
        if site_type == 'build-to-suit':
            log_build_to_suit_task(colopriming_site, self.request.id, "ERROR", f"Attempt {self.request.retries + 1}/4 failed: {str(e)}")

        logger.error(error_msg)
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        logger.error(f"Traceback: {tb_str}")

        # Update record status to FAILURE on final failure
        if self.request.retries >= 3:
            try:
                loop = get_or_create_event_loop()
                loop.run_until_complete(update_record(pColopriming, record_id, {
                    'status': 'FAILURE',
                    'error_message': f"{error_msg}\n{tb_str}",
                    'site_type': site_type  # Include site_type in error record
                }))

                if site_type == 'build-to-suit':
                    log_build_to_suit_task(colopriming_site, self.request.id, "FAILED", "Max retries exceeded")

            except Exception as update_error:
                logger.warning(f"⚠️  Warning: Could not update failure status: {update_error}")

        raise self.retry(exc=e)

async def async_colopriming_analysis_siro(colopriming_site, record_id, task_id, task):
    await asyncio.sleep(1)  # Properly await sleep
    update = {'task_id': task_id, 'status': "STARTED",'created_at' : datetime.now()}
    await update_record(pSiro,record_id, update)
    result = await colopriming_siro_analysis(colopriming_site, record_id, task_id, task, 'background', True)
    return result

@celery.task(bind=True, name='celery_colopriming_analysis_siro')
def celery_colopriming_analysis_siro(self, colopriming_site, record_id):
    try:
        task_id = self.request.id
        task = AsyncResult(task_id)
        logger.info(f"🚀 Starting siro analysis task: {task_id} for record: {record_id}")

        # Get or create event loop safely
        loop = get_or_create_event_loop()

        # Run the async function in the event loop
        result = loop.run_until_complete(async_colopriming_analysis_siro(colopriming_site, record_id, task_id, task))
        logger.info(f"✅ Siro analysis completed for record: {record_id}")
        return result
    except Exception as e:
        logger.error(f"❌ Siro analysis task failed: {str(e)}")
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        logger.error(f"Traceback: {tb_str}")
        raise e


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


@celery.task(bind=True, name='celery_bulk_colopriming_analysis')
def celery_bulk_colopriming_analysis(self, job_id, sites_data, output_filename):
    """
    Celery task for bulk colopriming analysis
    """
    try:
        task_id = self.request.id
        logger.info(f"🚀 Starting bulk analysis task: {task_id} for job: {job_id}")

        # Get or create event loop safely
        loop = get_or_create_event_loop()

        # Update job with task_id
        logger.info(f"📝 Updating job {job_id} with task_id: {task_id}")
        loop.run_until_complete(update_bulk_job(job_id, {'task_id': task_id}))

        # Run the async bulk analysis
        logger.info(f"⚡ Running bulk analysis for {len(sites_data)} sites")
        result = loop.run_until_complete(async_bulk_colopriming_analysis(job_id, sites_data, output_filename))

        logger.info(f"✅ Bulk analysis completed successfully for job: {job_id}")
        return result

    except Exception as e:
        error_msg = f"❌ Celery task failed: {str(e)}"
        logger.error(error_msg)
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        logger.error(f"Traceback: {tb_str}")

        # Try to update the job status to FAILURE
        try:
            loop = get_or_create_event_loop()
            loop.run_until_complete(update_bulk_job(job_id, {
                'status': 'FAILURE',
                'error_message': f"{error_msg}\n{tb_str}",
                'finished_at': datetime.now()
            }))
        except Exception as update_error:
            logger.error(f"Failed to update job status: {update_error}")

        raise e