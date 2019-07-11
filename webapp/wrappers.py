"""Wrappers to encapsulate image pulling tasks into a single Redis-Queue (RQ)
queueable job.

Used within app.py, these wrappers instantiate relevant image grabber
classes, schedule the pulling function inside an asyncio event loop,
and call, finally posting the records of uploaded images to a second
Redis database.

Though messy, the extra wrapper is required because:
1) RQ-enqueable functions must be importable from a module separate
from __main__;
2) Some classes instantiated (including DG auth, Google Cloud Storage) 
create complex local context and require asyncio event
scheduling that cannot be pickled and therefore cannot be queued.
Via these wrappers, the context is created only in the worker process.
"""
import json
import os

import redis

from grabbers.base import loop
from grabbers.dg import DGImageGrabber
from grabbers.landsat import LandsatThumbnails
from grabbers.planet_grabber import PlanetGrabber


# Heroku provides the env variable REDIS_URL for Heroku redis;
# the default redis://redis_db:6379 points to the local docker redis
redis_url = os.getenv('REDIS_URL', 'redis://redis_db:6379')
connection = redis.from_url(redis_url, decode_responses=True)

PROVIDER_CLASSES = {
    'digital_globe': DGImageGrabber,
    'landsat': LandsatThumbnails,
    'planet': PlanetGrabber
}

def pull(db_key, provider, bbox, **specs):
    """Pull an image."""
    grabber = PROVIDER_CLASSES[provider](**specs)
    looped = loop(grabber.pull)
    records = looped(bbox)
    reformatted = _format_exceptions(*records)
    connection.set(db_key, json.dumps(reformatted))
    return records

def pull_by_id(db_key, provider, bbox, catalogID, item_type, **specs):
    """Pull an image for a known catalogID."""
    grabber = PROVIDER_CLASSES[provider](**specs)
    looped = loop(grabber.pull_by_id)
    record = looped(bbox, catalogID, item_type)
    reformatted = _format_exceptions(record)
    connection.set(db_key, json.dumps(reformatted))
    return record

def _format_exceptions(*results):
    """Format returned exceptions to be JSON serializable.""" 
    formatted = []
    for r in results:
        if isinstance(r, Exception):
            formatted.append('Returned by asyncio.gather: {}'.format(repr(r)))
        else:
            formatted.append(r)
    return formatted
