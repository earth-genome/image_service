import os

import redis
from rq import Worker, Queue, Connection

listen = ['default']

redis_url = os.getenv('REDISTOGO_URL', 'redis://redis:6379')
connection = redis.from_url(redis_url)

if __name__ == '__main__':
    with Connection(connection):
        worker = Worker(map(Queue, listen))
        worker.work()
