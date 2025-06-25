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
celery.conf.worker_max_tasks_per_child = 1000
celery.conf.worker_concurrency = int(os.environ.get('CELERY_WORKER_CONCURRENCY', 4))

# Ignore unknown tasks
celery.conf.task_ignore_result = False
celery.conf.task_store_errors_even_if_ignored = True

# Only accept tasks that are registered
celery.conf.task_routes = {
    'celery_colopriming_analysis': {'queue': 'celery'},
    'celery_colopriming_analysis_siro': {'queue': 'celery'},
    'celery_bulk_colopriming_analysis': {'queue': 'celery'},
}

# Handle unknown/unregistered tasks
celery.conf.task_ignore_result = False
celery.conf.task_store_errors_even_if_ignored = True

# Reject unknown tasks
celery.conf.task_reject_on_worker_lost = True

# Add task filtering to ignore known problematic tasks
celery.conf.task_ignore_results = [
    'reports.scheduler',  # Ignore reports.scheduler task
]

# Configure worker to only process known tasks
celery.conf.worker_direct = True
celery.conf.task_default_queue = 'celery'
celery.conf.task_default_exchange = 'celery'
celery.conf.task_default_exchange_type = 'direct'
celery.conf.task_default_routing_key = 'celery'

# Increase task rejection timeout
celery.conf.task_reject_on_worker_lost = True
celery.conf.task_acks_late = True
celery.conf.worker_prefetch_multiplier = 1

# Add additional worker settings for stability
celery.conf.worker_disable_rate_limits = True
celery.conf.task_always_eager = False
celery.conf.task_eager_propagates = True

# List of known problematic tasks to ignore
IGNORED_TASKS = [
    'reports.scheduler',
    'periodic_tasks.cleanup',
    'maintenance.scheduler',
]

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

# Custom task handler for unknown tasks
@celery.signals.task_unknown.connect
def task_unknown_handler(sender=None, name=None, id=None, message=None, exc=None, **kwargs):
    """Handle unknown tasks gracefully"""
    if name in IGNORED_TASKS:
        logger.info(f"🔇 Ignoring known problematic task: {name} (ID: {id})")
        return

    logger.warning(f"⚠️  Unknown task received: {name} (ID: {id})")
    logger.debug(f"Task message: {message}")

# Handle task failures more gracefully
@celery.signals.task_failure.connect
def task_failure_handler(sender=None, task_id=None, exception=None, traceback=None, einfo=None, **kwargs):
    """Enhanced task failure handling"""
    logger.error(f"💥 Task failed: {sender} (ID: {task_id})")
    logger.error(f"Exception: {exception}")
    if traceback:
        logger.error(f"Traceback: {traceback}")

# Log successful tasks
@celery.signals.task_success.connect
def task_success_handler(sender=None, result=None, **kwargs):
    """Log successful task completion"""
    task_name = sender.__name__ if hasattr(sender, '__name__') else str(sender)
    if 'colopriming' in task_name.lower():
        logger.info(f"✅ Task completed successfully: {task_name}")

# Worker ready signal
@celery.signals.worker_ready.connect
def worker_ready_handler(sender=None, **kwargs):
    """Log when worker is ready"""
    logger.info("🚀 DIONET Celery worker is ready!")
    logger.info(f"Registered tasks: {list(celery.tasks.keys())}")
    logger.info(f"Ignored tasks: {IGNORED_TASKS}")

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

        # Create new event loop for this task
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        # Update task status to STARTED immediately
        try:
            loop.run_until_complete(update_record(pColopriming, record_id, {
                'status': 'STARTED',
                'task_id': task_id
            }))
        except Exception as update_error:
            print(f"⚠️  Warning: Could not update task status: {update_error}")

        # Run the async function in the event loop
        result = loop.run_until_complete(async_colopriming_analysis(colopriming_site, record_id, task_id, task))
        print(f"✅ Colopriming analysis completed for record: {record_id}")
        return result

    except Exception as e:
        site_type = colopriming_site.get('site_type', 'unknown')
        error_msg = f"❌ Colopriming analysis task failed ({site_type}): {str(e)}"

        # Enhanced error logging for build-to-suit
        if site_type == 'build-to-suit':
            log_build_to_suit_task(colopriming_site, task_id, "ERROR", f"Attempt {self.request.retries + 1}/4 failed: {str(e)}")

        logger.error(error_msg)
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        logger.error(f"Traceback: {tb_str}")

        # Update record status to FAILURE on final failure
        if self.request.retries >= 3:
            try:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(update_record(pColopriming, record_id, {
                    'status': 'FAILURE',
                    'error_message': f"{error_msg}\n{tb_str}",
                    'site_type': site_type  # Include site_type in error record
                }))

                if site_type == 'build-to-suit':
                    log_build_to_suit_task(colopriming_site, task_id, "FAILED", "Max retries exceeded")

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
        print(f"🚀 Starting siro analysis task: {task_id} for record: {record_id}")

        # Create new event loop for this task
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        # Run the async function in the event loop
        result = loop.run_until_complete(async_colopriming_analysis_siro(colopriming_site, record_id, task_id, task))
        print(f"✅ Siro analysis completed for record: {record_id}")
        return result
    except Exception as e:
        print(f"❌ Siro analysis task failed: {str(e)}")
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        print(f"Traceback: {tb_str}")
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
        print(f"🚀 Starting bulk analysis task: {task_id} for job: {job_id}")

        # Create new event loop for this task
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        # Update job with task_id
        print(f"📝 Updating job {job_id} with task_id: {task_id}")
        loop.run_until_complete(update_bulk_job(job_id, {'task_id': task_id}))

        # Run the async bulk analysis
        print(f"⚡ Running bulk analysis for {len(sites_data)} sites")
        result = loop.run_until_complete(async_bulk_colopriming_analysis(job_id, sites_data, output_filename))

        print(f"✅ Bulk analysis completed successfully for job: {job_id}")
        return result

    except Exception as e:
        error_msg = f"❌ Celery task failed: {str(e)}"
        print(error_msg)
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        print(f"Traceback: {tb_str}")

        # Try to update the job status to FAILURE
        try:
            if 'loop' in locals():
                loop.run_until_complete(update_bulk_job(job_id, {
                    'status': 'FAILURE',
                    'error_message': f"{error_msg}\n{tb_str}",
                    'finished_at': datetime.now()
                }))
        except Exception as update_error:
            print(f"Failed to update job status: {update_error}")

        raise e