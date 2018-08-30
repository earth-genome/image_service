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

# Heroku provides the env variable REDIS_URL for Heroku redis;
# the default redis://redis_db:6379 points to the local docker redis
redis_url = os.getenv('REDIS_URL', 'redis://redis_db:6379')
connection = redis.from_url(redis_url, decode_responses=True)

from grab_imagery import grabber_handlers

def pull(db_key, bbox, **specs):
    """Pull an image."""
    grabber = grabber_handlers.GrabberHandler(**specs)
    looped = grabber_handlers.loop(grabber.pull)
    records = looped(bbox)
    connection.set(db_key, json.dumps(records))
    return records

def pull_by_id(db_key, bbox, catalogID, item_type, **specs):
    """Pull an image for a known catalogID."""
    provider = specs['providers'][0]
    grabber = grabber_handlers.GrabberHandler(**specs)
    looped = grabber_handlers.loop(grabber.pull_by_id)
    records = looped(provider, bbox, catalogID, item_type)
    connection.set(db_key, json.dumps(records))
    return records

def pull_for_story(db_key, story, **specs):
    """Pull images for a story in the WTL database."""
    grabber = grabber_handlers.StoryHandler(**specs)
    looped = grabber_handlers.loop(grabber.pull_for_story)
    records = looped(story)
    connection.set(db_key, json.dumps(records))
    return records 
