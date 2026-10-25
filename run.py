"""Start a local cluster; Ctrl+C stops every child process."""
import subprocess
import sys
import time
from pathlib import Path

root = Path(__file__).resolve().parent
children = []
try:
    for port in (8101, 8102, 8103):
        children.append(subprocess.Popen([sys.executable, str(root / 'cache.py'), 'node', '--port', str(port)]))
    time.sleep(0.5)
    children.append(subprocess.Popen([sys.executable, str(root / 'cache.py'), 'gateway', '--port', '8080']))
    print('Cache API: http://127.0.0.1:8080 | status: /status', flush=True)
    while all(p.poll() is None for p in children):
        time.sleep(0.5)
except KeyboardInterrupt:
    pass
finally:
    for child in children:
        child.terminate()
    for child in children:
        child.wait()
