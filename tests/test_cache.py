import json
import random
import socket
import subprocess
import sys
import threading
import time
import unittest
import urllib.error
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cache import Ring, Node, rpc


class UnitTests(unittest.TestCase):
    def test_distinct_replicas(self):
        for i in range(100):
            self.assertEqual(len(set(Ring(['a', 'b', 'c']).owners(str(i)))), 2)

    def test_minimal_primary_movement(self):
        before, after = Ring(['a', 'b', 'c']), Ring(['a', 'b', 'c', 'd'])
        moved = 0
        for i in range(1000):
            old, new = before.owners(str(i), 1), after.owners(str(i), 1)
            if old != new:
                moved += 1
                self.assertEqual(new, ['d'])
        self.assertTrue(100 < moved < 400)

    def test_stale_replica_update_rejected(self):
        node = Node()
        node.handle('POST', '/internal/x', {'value': 2, 'version': 2})
        node.handle('POST', '/internal/x', {'value': 1, 'version': 1})
        self.assertEqual(node.handle('GET', '/internal/x', None)[1]['value'], 2)


class IntegrationTests(unittest.TestCase):
    def test_live_failure_recovery_join_and_ttl(self):
        # Reserve ports together to avoid choosing duplicates.
        sockets = [socket.socket() for _ in range(5)]
        for s in sockets:
            s.bind(('127.0.0.1', 0))
        ports = [s.getsockname()[1] for s in sockets]
        for s in sockets:
            s.close()
        nodes = [f'http://127.0.0.1:{p}' for p in ports[:4]]
        gateway = f'http://127.0.0.1:{ports[4]}'
        children = []
        root = Path(__file__).resolve().parents[1]
        def start(role, port):
            child = subprocess.Popen([sys.executable, str(root / 'cache.py'), role, '--port', str(port), '--nodes', ','.join(nodes)], stdout=subprocess.DEVNULL)
            children.append(child)
            return child
        def wait_for(predicate):
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                try:
                    if predicate():
                        return
                except (OSError, ValueError):
                    pass
                time.sleep(0.05)
            self.fail('cluster readiness deadline exceeded')
        try:
            workers = [start('node', p) for p in ports[:3]]
            for node in nodes[:3]:
                wait_for(lambda: rpc(node, '/health')['ok'])
            start('gateway', ports[4])
            wait_for(lambda: len(rpc(gateway, '/status')['live']) == 3)
            for i in range(30):
                rpc(gateway, f'/cache/key{i}', {'value': i})
            errors, latencies = [], []
            def traffic():
                for i in range(100):
                    started = time.perf_counter()
                    try:
                        self.assertEqual(rpc(gateway, f'/cache/key{i % 30}')['value'], i % 30)
                        rpc(gateway, f'/cache/new{i}', {'value': i})
                    except Exception as exc:
                        errors.append(str(exc))
                    latencies.append((time.perf_counter() - started) * 1000)
            thread = threading.Thread(target=traffic)
            thread.start()
            time.sleep(0.05)
            victim = random.Random(42).randrange(3)
            workers[victim].kill()
            workers[victim].wait()
            thread.join()
            self.assertEqual(errors, [])
            wait_for(lambda: len(rpc(gateway, '/status')['live']) == 2)
            start('node', ports[victim])
            wait_for(lambda: len(rpc(gateway, '/status')['live']) == 3)
            start('node', ports[3])
            wait_for(lambda: len(rpc(gateway, '/status')['live']) == 4)
            def replicated():
                snapshots = {n: rpc(n, '/snapshot') for n in nodes}
                return all(f'key{i}' in snapshots[n] for i in range(30) for n in Ring(nodes).owners(f'key{i}'))
            wait_for(replicated)
            rpc(gateway, '/cache/expiry', {'value': 'old'})
            rpc(gateway, '/cache/expiry', {'value': 'new', 'ttl': 0.15})
            time.sleep(0.25)
            with self.assertRaises(urllib.error.HTTPError) as caught:
                rpc(gateway, '/cache/expiry')
            self.assertEqual(caught.exception.code, 404)
            with self.assertRaises(urllib.error.HTTPError) as caught:
                rpc(gateway, '/cache/invalid', {'value': 1, 'ttl': -1})
            self.assertEqual(caught.exception.code, 400)
            report = {'traffic_operations': 200, 'failed_operations': len(errors), 'client_retries': 0, 'killed_node_index': victim, 'read_write_pair_p95_ms': round(sorted(latencies)[94], 2), 'recovered_and_joined_nodes': 4, 'metrics': rpc(gateway, '/status')['metrics']}
            print('\nCHAOS_RESULT ' + json.dumps(report), flush=True)
        finally:
            for child in children:
                if child.poll() is None:
                    child.terminate()
            for child in children:
                child.wait(timeout=5)


if __name__ == '__main__':
    unittest.main(verbosity=2)
