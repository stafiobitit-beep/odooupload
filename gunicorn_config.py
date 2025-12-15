import multiprocessing
import os

# Server socket
bind = f"0.0.0.0:{os.environ.get('PORT', '10000')}"
backlog = 2048

# Worker processes
# Default to 1 to conserve memory on Render basic plans
# (1 worker ~ 150MB ram vs 3 workers ~ 450MB ram)
web_concurrency = os.environ.get("WEB_CONCURRENCY")
if web_concurrency:
    workers = int(web_concurrency)
else:
    workers = 1
worker_class = 'sync'
worker_connections = 1000
timeout = 120
keepalive = 5

# Logging
accesslog = '-'
errorlog = '-'
loglevel = 'info'
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'

# Process naming
proc_name = 'odoo-excel-uploader'

# Server mechanics
daemon = False
pidfile = None
umask = 0
user = None
group = None
tmp_upload_dir = None

# SSL (not needed on Render)
keyfile = None
certfile = None
