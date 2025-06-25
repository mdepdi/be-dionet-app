#!/usr/bin/env python3
"""
Script to check Celery worker status and Redis connection
"""
import os
import redis
import traceback
from celery import Celery
from datetime import datetime

def check_redis_connection():
    """Check if Redis is accessible"""
    try:
        redis_url = os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0')
        print(f"🔍 Checking Redis connection: {redis_url}")

        r = redis.from_url(redis_url)
        r.ping()

        # Get Redis info
        info = r.info()
        print(f"✅ Redis connected successfully")
        print(f"   - Redis version: {info.get('redis_version')}")
        print(f"   - Connected clients: {info.get('connected_clients')}")
        print(f"   - Used memory: {info.get('used_memory_human')}")

        return True

    except Exception as e:
        print(f"❌ Redis connection failed: {str(e)}")
        return False

def check_celery_workers():
    """Check Celery worker status"""
    try:
        # Initialize Celery app
        celery_app = Celery('celery_worker')
        celery_app.conf.broker_url = os.environ.get('CELERY_BROKER_URL')
        celery_app.conf.result_backend = os.environ.get('CELERY_RESULT_BACKEND')

        print(f"🔍 Checking Celery workers...")

        # Get active workers
        inspect = celery_app.control.inspect()

        # Check active workers
        active_workers = inspect.active()
        if active_workers:
            print(f"✅ Active workers found: {len(active_workers)}")
            for worker_name, tasks in active_workers.items():
                print(f"   - Worker: {worker_name}")
                print(f"     Active tasks: {len(tasks)}")
        else:
            print("❌ No active workers found")

        # Check registered tasks
        registered = inspect.registered()
        if registered:
            print(f"📋 Registered tasks:")
            for worker_name, tasks in registered.items():
                print(f"   - Worker: {worker_name}")
                for task in tasks:
                    if 'colopriming' in task:
                        print(f"     📍 {task}")

        # Check worker stats
        stats = inspect.stats()
        if stats:
            print(f"📊 Worker statistics:")
            for worker_name, stat in stats.items():
                print(f"   - Worker: {worker_name}")
                print(f"     Pool: {stat.get('pool', {}).get('max-concurrency', 'N/A')}")
                print(f"     Total tasks: {stat.get('total', {})}")

        return bool(active_workers)

    except Exception as e:
        print(f"❌ Celery worker check failed: {str(e)}")
        tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        print(f"Traceback: {tb_str}")
        return False

def check_task_queues():
    """Check task queues in Redis"""
    try:
        redis_url = os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0')
        r = redis.from_url(redis_url)

        print(f"📨 Checking task queues:")

        # Check default queue
        queue_length = r.llen('celery')
        print(f"   - Default queue (celery): {queue_length} tasks")

        # Check for any other queues
        keys = r.keys('*queue*')
        for key in keys:
            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
            if key_str != 'celery':
                length = r.llen(key)
                print(f"   - Queue ({key_str}): {length} tasks")

        # Check for stuck tasks
        processing_keys = r.keys('*processing*')
        if processing_keys:
            print(f"⚠️  Found {len(processing_keys)} potentially stuck processing tasks")

        return True

    except Exception as e:
        print(f"❌ Queue check failed: {str(e)}")
        return False

def main():
    print(f"🚀 Celery Health Check - {datetime.now()}")
    print("=" * 50)

    # Load environment variables
    print(f"🔧 Environment variables:")
    print(f"   - CELERY_BROKER_URL: {os.environ.get('CELERY_BROKER_URL', 'Not set')}")
    print(f"   - CELERY_RESULT_BACKEND: {os.environ.get('CELERY_RESULT_BACKEND', 'Not set')}")
    print()

    # Check Redis
    redis_ok = check_redis_connection()
    print()

    # Check Celery workers
    workers_ok = check_celery_workers()
    print()

    # Check queues
    queues_ok = check_task_queues()
    print()

    # Summary
    print("=" * 50)
    print(f"📋 Summary:")
    print(f"   - Redis: {'✅ OK' if redis_ok else '❌ FAILED'}")
    print(f"   - Workers: {'✅ OK' if workers_ok else '❌ FAILED'}")
    print(f"   - Queues: {'✅ OK' if queues_ok else '❌ FAILED'}")

    if not (redis_ok and workers_ok):
        print()
        print("🔧 Suggested actions:")
        if not redis_ok:
            print("   - Check if Redis server is running")
            print("   - Verify CELERY_BROKER_URL environment variable")
        if not workers_ok:
            print("   - Start Celery worker: celery -A celery_worker.celery worker --loglevel=info")
            print("   - Check worker logs for errors")

if __name__ == "__main__":
    main()