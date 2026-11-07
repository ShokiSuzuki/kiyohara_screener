"""
Run this script to start an RQ worker for the 'kiyohara' queue.

Usage:
  python worker.py

Or use the RQ CLI: `rq worker kiyohara` (with REDIS_URL set in env)
"""
import os
import time
from dotenv import load_dotenv
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from rq import Worker, Queue

load_dotenv()

# Read REDIS_URL from env, prefer the docker-compose service name by default
redis_url = os.environ.get('REDIS_URL', 'redis://redis:6379/0')

def wait_for_redis(url, timeout=15, interval=1):
  deadline = time.time() + timeout
  last_exc = None
  while time.time() < deadline:
    try:
      conn = Redis.from_url(url)
      conn.ping()
      return conn
    except Exception as e:
      last_exc = e
      time.sleep(interval)
  raise last_exc

# Try primary URL, then fallback to a service-hosted address if that fails.
try:
  conn = wait_for_redis(redis_url, timeout=15)
except Exception:
  # If the provided URL points at localhost (common when .env on host was used),
  # try replacing localhost/127.0.0.1 with the compose service name `redis`.
  fallback = redis_url.replace('localhost', 'redis').replace('127.0.0.1', 'redis')
  if fallback != redis_url:
    try:
      conn = wait_for_redis(fallback, timeout=15)
    except Exception:
      conn = None
  else:
    conn = None

  if conn is None:
    try:
      conn = wait_for_redis('redis://redis:6379/0', timeout=15)
    except Exception as e:
      print('ERROR: could not connect to Redis:', e)
      raise


def main():
  listen = ['kiyohara']
  # Set job timeout to 1 hour (3600 seconds) on the Queue so jobs inherit it
  queues = [Queue(name, connection=conn, default_timeout=3600) for name in listen]
  # Worker does not accept a `job_timeout` kwarg; create normally
  worker = Worker(queues, connection=conn)
  worker.work()


if __name__ == '__main__':
  print('Using REDIS_URL=', redis_url)
  main()
