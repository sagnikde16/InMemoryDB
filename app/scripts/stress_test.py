import asyncio
import time
import random
import argparse
from collections import Counter

# Stress Test Script for InMemoryDB
# Simulates Chaos: Tests connection limits, new commands (LPUSH, HSET), TTLs, and heavy concurrency.

COMMAND_WEIGHTS = {
    "SET": 0.20,
    "GET": 0.30,
    "LPUSH": 0.15,
    "LRANGE": 0.10,
    "HSET": 0.15,
    "HGET": 0.10
}

async def chaos_worker(worker_id, host, port, requests_per_worker, stats):
    try:
        reader, writer = await asyncio.open_connection(host, port)
        stats['connections_established'] += 1
    except Exception as e:
        stats['errors']['connection_failed'] += 1
        return

    commands = list(COMMAND_WEIGHTS.keys())
    weights = list(COMMAND_WEIGHTS.values())
    
    for i in range(requests_per_worker):
        cmd = random.choices(commands, weights=weights)[0]
        key = f"stress:{worker_id}:{i % 100}" # High overlap to hit same shards
        
        if cmd == "SET":
            payload = f"SET {key} some_random_value_for_stress_testing_length {random.randint(1, 10)}\r\n"
        elif cmd == "GET":
            payload = f"GET {key}\r\n"
        elif cmd == "LPUSH":
            payload = f"LPUSH list:{key} item_{i}\r\n"
        elif cmd == "LRANGE":
            payload = f"LRANGE list:{key} 0 10\r\n"
        elif cmd == "HSET":
            payload = f"HSET hash:{key} field_{i%10} val_{i}\r\n"
        elif cmd == "HGET":
            payload = f"HGET hash:{key} field_{i%10}\r\n"

        try:
            writer.write(payload.encode())
            await writer.drain()
            
            response = await reader.readline()
            if not response:
                stats['errors']['disconnects'] += 1
                break
                
            resp_str = response.decode().strip()
            if resp_str.startswith("-ERR"):
                stats['errors'][resp_str] += 1
            else:
                stats['success'] += 1
                
        except Exception as e:
            stats['errors']['socket_error'] += 1
            break

    writer.close()
    await writer.wait_closed()

async def run_stress_test(args):
    stats = {
        'connections_established': 0,
        'success': 0,
        'errors': Counter()
    }
    
    requests_per_worker = args.requests // args.concurrency

    print(f"[*] Starting Stress Test (Chaos Mode) against {args.host}:{args.port}")
    print(f"[*] Simulating {args.concurrency} concurrent clients, {args.requests} total requests...")
    print(f"[*] Testing new B1 commands (LPUSH, LRANGE, HSET, HGET)...")
    
    start_time = time.perf_counter()

    # Launch all workers concurrently to slam the server
    tasks = [
        chaos_worker(i, args.host, args.port, requests_per_worker, stats)
        for i in range(args.concurrency)
    ]
    await asyncio.gather(*tasks)

    duration = time.perf_counter() - start_time

    print("\n" + "="*40)
    print("         STRESS TEST RESULTS")
    print("="*40)
    print(f"Test Duration:           {duration:.2f} seconds")
    print(f"Connections Established: {stats['connections_established']:,} / {args.concurrency:,}")
    print(f"Successful Requests:     {stats['success']:,}")
    
    print(f"\n--- Errors & Rejections ---")
    if not stats['errors']:
        print("Zero errors! Perfect stability.")
    else:
        for err_type, count in stats['errors'].items():
            print(f"{err_type}: {count:,}")
            
    if stats['errors'].get("-ERR rate limit exceeded", 0) > 0:
        print("\n[!] The server actively rate-limited the connections (Expected for localhost).")
    
    if stats['errors'].get("-ERR max connections reached", 0) > 0:
        print("\n[!] You hit the B5 MAX_CONNECTIONS limit!")
        
    print("========================================\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="InMemoryDB Stress Test Tool")
    parser.add_argument("--host", default="127.0.0.1", help="Server Host")
    parser.add_argument("--port", type=int, default=8080, help="Server Port")
    parser.add_argument("-c", "--concurrency", type=int, default=1000, help="Number of concurrent workers")
    parser.add_argument("-r", "--requests", type=int, default=1000000, help="Total number of requests")
    args = parser.parse_args()
    
    asyncio.run(run_stress_test(args))