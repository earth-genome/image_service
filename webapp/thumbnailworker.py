import os

import redis
from rq import Worker, Queue, Connection
from rq.handlers import move_to_failed_queue

from exception_handler import post_to_db

listen = ['thumbnails']

# Heroku provides the env variable REDISTOGO_URL for Heroku RedisToGo;
# the default redis://redis_worker:6379 points to the local docker redis
redis_url = os.getenv('REDISTOGO_URL', 'redis://redis_worker:6379')
connection = redis.from_url(redis_url)

if __name__ == '__main__':
    with Connection(connection):
        worker = Worker(
            map(Queue, listen),
            exception_handlers=[post_to_db, move_to_failed_queue])
        worker.work()
