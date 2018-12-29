"""Wrappers to encapsulate image pulling tasks into a single Redis-Queue (RQ)
queueable job.

Used within app.py, these wrappers instantiate relevant image grabber
classes, schedule the pulling function inside an asyncio event loop,
and call, finally posting the records of uploaded images to a second
Redis database.

Though messy, the extra wrapper is required because:
1) RQ-enqueable functions must be importable from a module separate
from __main__;
2) The classes instantiated in grabber_handlers (including DG auth,
Google Cloud Storage) create complex local context and require asyncio event
scheduling that cannot be pickled and therefore cannot be queued.

Via these wrappers, the context is created only in the worker process.
"""
import json
import os

import redis

from grab_imagery import dg_grabber
from grab_imagery.grabber import loop
from grab_imagery.landsat import landsat_grabber
from grab_imagery import planet_grabber 

# Heroku provides the env variable REDIS_URL for Heroku redis;
# the default redis://redis_db:6379 points to the local docker redis
redis_url = os.getenv('REDIS_URL', 'redis://redis_db:6379')
connection = redis.from_url(redis_url, decode_responses=True)

PROVIDER_CLASSES = {
    'digital_globe': dg_grabber.DGImageGrabber,
    'landsat': landsat_grabber.LandsatThumbnails,
    'planet': planet_grabber.PlanetGrabber
}

def pull(db_key, provider, bbox, **specs):
    """Pull an image."""
    grabber = PROVIDER_CLASSES[provider](**specs)
    looped = loop(grabber.pull)
    records = looped(bbox)
    connection.set(db_key, json.dumps(records))
    return records

def pull_by_id(db_key, provider, bbox, catalogID, item_type, **specs):
    """Pull an image for a known catalogID."""
    grabber = PROVIDER_CLASSES[provider](**specs)
    looped = loop(grabber.pull_by_id)
    record = looped(bbox, catalogID, item_type)
    connection.set(db_key, json.dumps(record))
    return record
