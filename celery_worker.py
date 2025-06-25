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

# Create celery app with unique name for this application
celery = Celery('dionet_worker')
celery.conf.broker_url = os.environ.get('CELERY_BROKER_URL')
celery.conf.result_backend = os.environ.get('CELERY_RESULT_BACKEND')
celery.conf.task_serializer = 'json'
celery.conf.accept_content = ['json']
celery.conf.result_serializer = 'json'
celery.conf.timezone = 'UTC'
celery.conf.enable_utc = True

# QUEUE ISOLATION - Use specific queues to avoid conflicts with other apps
DIONET_QUEUE_NAME = 'dionet_queue'
celery.conf.task_default_queue = DIONET_QUEUE_NAME
celery.conf.task_default_exchange = 'dionet_exchange'
celery.conf.task_default_exchange_type = 'direct'
celery.conf.task_default_routing_key = 'dionet.tasks'

# COMPLETELY DISABLE all event monitoring and remote control
celery.conf.worker_send_task_events = False
celery.conf.task_send_sent_event = False
celery.conf.task_track_started = False
celery.conf.worker_enable_remote_control = False
celery.conf.worker_disable_rate_limits = True

# Set task time limits
celery.conf.task_time_limit = int(os.environ.get('CELERY_TASK_TIME_LIMIT', 3600))
celery.conf.task_soft_time_limit = int(os.environ.get('CELERY_TASK_SOFT_TIME_LIMIT', 3300))

# Simplified worker settings for solo pool
celery.conf.worker_prefetch_multiplier = 1
celery.conf.worker_max_tasks_per_child = 1000
celery.conf.worker_concurrency = 1

# Task handling - STRICT mode to reject unknown tasks
celery.conf.task_ignore_result = False
celery.conf.task_store_errors_even_if_ignored = False
celery.conf.task_reject_on_worker_lost = True
celery.conf.task_always_eager = False
celery.conf.task_eager_propagates = True

# CRITICAL: STRICT routing - ONLY accept tasks from our specific queue
celery.conf.task_routes = {
    'dionet_worker.celery_colopriming_analysis': {
        'queue': DIONET_QUEUE_NAME,
        'routing_key': 'dionet.colopriming'
    },
    'dionet_worker.celery_colopriming_analysis_siro': {
        'queue': DIONET_QUEUE_NAME,
        'routing_key': 'dionet.siro'
    },
    'dionet_worker.celery_bulk_colopriming_analysis': {
        'queue': DIONET_QUEUE_NAME,
        'routing_key': 'dionet.bulk'
    },
}

# IMPORTANT: Completely ignore unknown tasks instead of processing them
celery.conf.task_ignore_result = True
celery.conf.task_store_errors_even_if_ignored = False

# REJECT unknown tasks completely - do not log them as errors
celery.conf.worker_hijack_root_logger = False
celery.conf.worker_log_color = False

def get_or_create_event_loop():
    """
    Safely get or create an event loop for async operations
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError("Event loop is closed")
        except RuntimeError:
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
    await asyncio.sleep(1)

    if colopriming_site.get('site_type') == 'build-to-suit':
        log_build_to_suit_task(colopriming_site, task_id, "PROCESSING", "Starting build-to-suit analysis")

    update = {'task_id': task_id, 'status': "STARTED",'created_at' : datetime.now()}
    await update_record(pColopriming,record_id, update)
    result = await colopriming_analysis(colopriming_site, record_id, task_id, task, 'background', False)
    return result

@celery.task(bind=True, name='dionet_worker.celery_colopriming_analysis',
              autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 60})
def celery_colopriming_analysis(self, colopriming_site, record_id):
    try:
        task_id = self.request.id
        task = AsyncResult(task_id)
        retries = self.request.retries

        site_type = colopriming_site.get('site_type', 'unknown')
        if site_type == 'build-to-suit':
            log_build_to_suit_task(colopriming_site, task_id, "STARTED", f"Attempt {retries + 1}/4")

        logger.info(f"🚀 [DIONET] Starting colopriming analysis task: {task_id} for record: {record_id} (attempt {retries + 1}/4) | Site Type: {site_type}")

        loop = get_or_create_event_loop()

        try:
            loop.run_until_complete(update_record(pColopriming, record_id, {
                'status': 'STARTED',
                'task_id': task_id
            }))
        except Exception as update_error:
            logger.warning(f"⚠️  Warning: Could not update task status: {update_error}")

        result = loop.run_until_complete(async_colopriming_analysis(colopriming_site, record_id, task_id, task))
        logger.info(f"✅ [DIONET] Colopriming analysis completed for record: {record_id}")
        return result

    except Exception as e:
        site_type = colopriming_site.get('site_type', 'unknown')
        error_msg = f"❌ [DIONET] Colopriming analysis task failed ({site_type}): {str(e)}"

        if site_type == 'build-to-suit':
            log_build_to_suit_task(colopriming_site, self.request.id, "ERROR", f"Attempt {self.request.retries + 1}/4 failed: {str(e)}")

        logger.error(error_msg)
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        logger.error(f"Traceback: {tb_str}")

        if self.request.retries >= 3:
            try:
                loop = get_or_create_event_loop()
                loop.run_until_complete(update_record(pColopriming, record_id, {
                    'status': 'FAILURE',
                    'error_message': f"{error_msg}\n{tb_str}",
                    'site_type': site_type
                }))

                if site_type == 'build-to-suit':
                    log_build_to_suit_task(colopriming_site, self.request.id, "FAILED", "Max retries exceeded")

            except Exception as update_error:
                logger.warning(f"⚠️  Warning: Could not update failure status: {update_error}")

        raise self.retry(exc=e)

async def async_colopriming_analysis_siro(colopriming_site, record_id, task_id, task):
    await asyncio.sleep(1)
    update = {'task_id': task_id, 'status': "STARTED",'created_at' : datetime.now()}
    await update_record(pSiro,record_id, update)
    result = await colopriming_siro_analysis(colopriming_site, record_id, task_id, task, 'background', True)
    return result

@celery.task(bind=True, name='dionet_worker.celery_colopriming_analysis_siro')
def celery_colopriming_analysis_siro(self, colopriming_site, record_id):
    try:
        task_id = self.request.id
        task = AsyncResult(task_id)
        logger.info(f"🚀 [DIONET] Starting siro analysis task: {task_id} for record: {record_id}")

        loop = get_or_create_event_loop()
        result = loop.run_until_complete(async_colopriming_analysis_siro(colopriming_site, record_id, task_id, task))
        logger.info(f"✅ [DIONET] Siro analysis completed for record: {record_id}")
        return result
    except Exception as e:
        logger.error(f"❌ [DIONET] Siro analysis task failed: {str(e)}")
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        logger.error(f"Traceback: {tb_str}")
        raise e

async def async_bulk_colopriming_analysis(job_id, sites_data, output_filename):
    """
    Process bulk colopriming analysis asynchronously
    """
    try:
        await update_bulk_job(job_id, {
            'status': 'PROCESSING',
            'started_at': datetime.now()
        })

        results = []
        total_sites = len(sites_data)
        processed_count = 0
        failed_count = 0

        job_details = await get_bulk_job_details(job_id)
        detail_lookup = {detail['site_index']: detail['id'] for detail in job_details}

        for idx, site_data in enumerate(sites_data):
            try:
                detail_id = detail_lookup.get(idx)

                if detail_id:
                    await update_bulk_job_detail(detail_id, {
                        'status': 'PROCESSING'
                    })

                result = await bulk_colopriming_analysis(site_data)

                if result and 'result' in result and len(result['result']) > 0:
                    for i, operator_result in enumerate(result['result']):
                        site_result = site_data.copy()
                        for k, v in operator_result.items():
                            site_result[k] = v
                        results.append(site_result)
                else:
                    results.append(site_data.copy())

                processed_count += 1

                if detail_id:
                    await update_bulk_job_detail(detail_id, {
                        'status': 'SUCCESS',
                        'processed_at': datetime.now()
                    })

            except Exception as site_error:
                failed_count += 1
                error_msg = str(site_error)

                site_result = site_data.copy()
                site_result['error'] = error_msg
                results.append(site_result)

                if detail_id:
                    await update_bulk_job_detail(detail_id, {
                        'status': 'FAILURE',
                        'error_message': error_msg,
                        'processed_at': datetime.now()
                    })

            progress = ((processed_count + failed_count) / total_sites) * 100
            await update_bulk_job(job_id, {
                'processed_sites': processed_count,
                'failed_sites': failed_count,
                'progress_percentage': progress
            })

        result_df = pd.DataFrame(results)
        output_dir = './dionet_data'
        os.makedirs(output_dir, exist_ok=True)
        output_path = f'{output_dir}/{output_filename}.csv'
        result_df.to_csv(output_path, index=False)

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

        await update_bulk_job(job_id, {
            'status': 'FAILURE',
            'finished_at': datetime.now(),
            'error_message': full_error
        })

        raise Exception(full_error)

@celery.task(bind=True, name='dionet_worker.celery_bulk_colopriming_analysis')
def celery_bulk_colopriming_analysis(self, job_id, sites_data, output_filename):
    """
    Celery task for bulk colopriming analysis
    """
    try:
        task_id = self.request.id
        logger.info(f"🚀 [DIONET] Starting bulk analysis task: {task_id} for job: {job_id}")

        loop = get_or_create_event_loop()

        logger.info(f"📝 [DIONET] Updating job {job_id} with task_id: {task_id}")
        loop.run_until_complete(update_bulk_job(job_id, {'task_id': task_id}))

        logger.info(f"⚡ [DIONET] Running bulk analysis for {len(sites_data)} sites")
        result = loop.run_until_complete(async_bulk_colopriming_analysis(job_id, sites_data, output_filename))

        logger.info(f"✅ [DIONET] Bulk analysis completed successfully for job: {job_id}")
        return result

    except Exception as e:
        error_msg = f"❌ [DIONET] Celery task failed: {str(e)}"
        logger.error(error_msg)
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        logger.error(f"Traceback: {tb_str}")

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