
web: gunicorn --bind 0.0.0.0:$PORT --worker-class quart.worker.GunicornUVLoopWorker --timeout 6000 wsgi

worker: python3 worker.py