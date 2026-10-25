"""Educational distributed cache: Python 3.11+, no external dependencies."""
import argparse
import bisect
import hashlib
import json
import math
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote


def rpc(url, path, data=None):
    request = urllib.request.Request(url + path, data=None if data is None else json.dumps(data).encode(), headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=0.6) as response:
        return json.load(response)


class Ring:
    def __init__(self, nodes, vnodes=128):
        self.points = sorted((self.hash(f'{node}:{i}'), node) for node in nodes for i in range(vnodes))
        self.hashes = [p[0] for p in self.points]

    @staticmethod
    def hash(key):
        return int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], 'big')

    def owners(self, key, count=2):
        result = []
        start = bisect.bisect_left(self.hashes, self.hash(key))
        for offset in range(len(self.points)):
            node = self.points[(start + offset) % len(self.points)][1]
            if node not in result:
                result.append(node)
            if len(result) == count:
                break
        return result


class Node:
    def __init__(self):
        self.records = {}
        self.lock = threading.RLock()

    def handle(self, method, path, body):
        with self.lock:
            if path == '/health':
                return 200, {'ok': True}
            if path == '/snapshot':
                return 200, self.records.copy()
            if path.startswith('/internal/'):
                key = unquote(path[len('/internal/'):])
                if method == 'POST':
                    old = self.records.get(key)
                    if old is None or body['version'] > old['version']:
                        self.records[key] = body
                    return 200, {'ok': True}
                return 200, self.records.get(key)
            return 404, {'error': 'unknown endpoint'}


class Cluster:
    def __init__(self, nodes, interval=0.5):
        self.nodes = nodes
        self.live = []
        self.lock = threading.RLock()
        self.version = time.time_ns()
        self.metrics = {'writes': 0, 'reads': 0, 'repairs': 0, 'failures_detected': 0}
        self.events = []
        self.interval = interval
        self.refresh()
        threading.Thread(target=self.monitor, daemon=True).start()

    def call(self, node, path, data=None):
        try:
            return True, rpc(node, path, data)
        except (OSError, ValueError):
            return False, None

    def refresh(self):
        # Serialized with client operations: no migration/write race.
        with self.lock:
            live = [n for n in self.nodes if self.call(n, '/health')[0]]
            if live != self.live:
                self.metrics['failures_detected'] += len(set(self.live) - set(live))
                self.events.append({'time': time.time(), 'live': live})
                self.events = self.events[-30:]
                self.live = live
            latest = {}
            for node in live:
                ok, records = self.call(node, '/snapshot')
                if ok:
                    for key, record in records.items():
                        if key not in latest or record['version'] > latest[key]['version']:
                            latest[key] = record
                            self.version = max(self.version, record['version'])
            ring = Ring(live)
            for key, record in latest.items():
                for node in ring.owners(key):
                    ok, old = self.call(node, '/internal/' + urllib.parse.quote(key, safe=''))
                    if ok and (old is None or old['version'] < record['version']):
                        if self.call(node, '/internal/' + urllib.parse.quote(key, safe=''), record)[0]:
                            self.metrics['repairs'] += 1

    def monitor(self):
        while True:
            time.sleep(self.interval)
            self.refresh()

    def handle(self, method, path, body):
        with self.lock:
            if path == '/health':
                return 200, {'ok': True}
            if path == '/status':
                return 200, {'nodes': self.nodes, 'live': self.live, 'metrics': self.metrics, 'events': self.events}
            if not path.startswith('/cache/'):
                return 404, {'error': 'use /cache/KEY or /status'}
            key = unquote(path[len('/cache/'):])
            if not key or len(key) > 1024:
                return 400, {'error': 'key must contain 1–1024 characters'}
            internal = '/internal/' + urllib.parse.quote(key, safe='')
            if method == 'POST':
                if not isinstance(body, dict) or 'value' not in body:
                    return 400, {'error': 'JSON value is required'}
                ttl = body.get('ttl')
                if ttl is not None and (isinstance(ttl, bool) or not isinstance(ttl, (int, float)) or not math.isfinite(ttl) or ttl <= 0):
                    return 400, {'error': 'ttl must be a positive finite number of seconds'}
                self.version = max(self.version + 1, time.time_ns())
                record = {'value': body['value'], 'version': self.version, 'expires_at': None if ttl is None else time.time() + ttl}
                # Try preferred owners, then fallback nodes if failure occurred before heartbeat.
                order = Ring(self.live).owners(key, len(self.live))
                successes = []
                for node in order:
                    if self.call(node, internal, record)[0]:
                        successes.append(node)
                    if len(successes) == 2:
                        break
                if len(successes) < 2:
                    return 503, {'error': 'two replica acknowledgments required; partial write may exist'}
                self.metrics['writes'] += 1
                return 200, {'ok': True, 'replicas': successes, 'version': record['version']}
            # Consult all configured nodes, including recovered nodes awaiting the next heartbeat.
            records = []
            reached = 0
            for node in self.nodes:
                ok, record = self.call(node, internal)
                reached += int(ok)
                if ok and record is not None:
                    records.append(record)
            if not reached:
                return 503, {'error': 'no reachable cache nodes'}
            record = max(records, key=lambda r: r['version']) if records else None
            self.metrics['reads'] += 1
            if record is None or (record['expires_at'] is not None and record['expires_at'] <= time.time()):
                return 404, {'error': 'cache miss'}
            return 200, record


def serve(app, port, host='127.0.0.1'):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            self.dispatch('GET')

        def do_POST(self):
            self.dispatch('POST')

        def dispatch(self, method):
            try:
                size = int(self.headers.get('Content-Length', 0))
                if size < 0 or size > 1048576:
                    status, result = 413, {'error': 'request too large'}
                else:
                    body = json.loads(self.rfile.read(size)) if size else None
                    status, result = app.handle(method, self.path, body)
            except (ValueError, TypeError, KeyError):
                status, result = 400, {'error': 'invalid request'}
            payload = json.dumps(result).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
    server = ThreadingHTTPServer((host, port), Handler)
    print(f'Listening on {host}:{port}', flush=True)
    server.serve_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('role', choices=['node', 'gateway'])
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--nodes', default='http://127.0.0.1:8101,http://127.0.0.1:8102,http://127.0.0.1:8103')
    args = parser.parse_args()
    serve(Node() if args.role == 'node' else Cluster(args.nodes.split(',')), args.port, args.host)
