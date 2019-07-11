
import json
from traceback import format_exception_only

import wrappers

def post_to_db(job, exc_type, exc_value, tb):
    """Post exception info to a Redis DB."""
    report = [{
        'Exception': format_exception_only(exc_type, exc_value)
    }]
    db_key = job.args[0]
    wrappers.connection.set(db_key, json.dumps(report))
    return
