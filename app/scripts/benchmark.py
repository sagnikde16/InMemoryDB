import asyncio
import time
import numpy as np
import argparse
from collections import Counter

# Advanced Benchmark Script for InMemoryDB
# Supports Ping-Pong (synchronous) and Pipelined modes.

async def client_worker(worker_id, host, port, requests_per_worker, pipeline_depth, latencies, errors):
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except Exception as e:
        errors['connection_failed'] += 1
        return

    req_count = 0
    while req_count < requests_per_worker:
        batch_size = min(pipeline_depth, requests_per_worker - req_count)
        
        # Prepare pipeline batch
        payload = ""
        for i in range(batch_size):
            key = f"bench:{worker_id}:{req_count + i}"
            payload += f"SET {key} val_{worker_id}_{req_count+i}\r\n"
        
        start_time = time.perf_counter()
        
        # Send pipelined requests
        writer.write(payload.encode())
        await writer.drain()
        
        # Read all responses for the pipeline
        for _ in range(batch_size):
            try:
                response = await reader.readline()
                if not response:
                    errors['disconnects'] += 1
                    break
                resp_str = response.decode().strip()
                if resp_str.startswith("-ERR"):
                    errors[resp_str] += 1
            except Exception:
                errors['read_errors'] += 1
                break
                
        # We record the average latency per request in this pipeline batch
        end_time = time.perf_counter()
        avg_latency = (end_time - start_time) / batch_size
        latencies.extend([avg_latency] * batch_size)
        
        req_count += batch_size

    writer.close()
    await writer.wait_closed()

async def run_benchmark(args):
    latencies = []
    errors = Counter()
    requests_per_worker = args.requests // args.concurrency

    print(f"[*] Starting Benchmark against {args.host}:{args.port}")
    print(f"[*] Mode: {'Pipelined (Depth: ' + str(args.pipeline) + ')' if args.pipeline > 1 else 'Ping-Pong'}")
    print(f"[*] Workers: {args.concurrency}, Total Requests: {args.requests}\n")
    
    start_bench = time.perf_counter()

    tasks = [
        client_worker(i, args.host, args.port, requests_per_worker, args.pipeline, latencies, errors)
        for i in range(args.concurrency)
    ]
    await asyncio.gather(*tasks)

    total_duration = time.perf_counter() - start_bench

    print("="*40)
    print("          BENCHMARK RESULTS")
    print("="*40)
    
    if latencies:
        latencies_ms = np.array(latencies) * 1000
        print(f"Total Requests:      {len(latencies):,}")
        print(f"Total Duration:      {total_duration:.2f} seconds")
        print(f"Throughput:          {len(latencies) / total_duration:,.2f} RPS")
        print(f"\n--- Latency Percentiles (Avg per batch) ---")
        print(f"p50 Latency:         {np.percentile(latencies_ms, 50):.3f} ms")
        print(f"p90 Latency:         {np.percentile(latencies_ms, 90):.3f} ms")
        print(f"p99 Latency:         {np.percentile(latencies_ms, 99):.3f} ms")
        print(f"Max Latency:         {np.max(latencies_ms):.3f} ms")
    
    print(f"\n--- Errors ---")
    if not errors:
        print("No errors encountered!")
    else:
        for err_type, count in errors.items():
            print(f"{err_type}: {count:,}")
            
    if errors.get("-ERR rate limit exceeded", 0) > 0:
        print("\n[!] WARNING: You hit the Rate Limiter (1000 req/min per IP).")
        print("    To test raw engine throughput, disable Rate Limiter in the server.")
    print("========================================\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="InMemoryDB Benchmark Tool")
    parser.add_argument("--host", default="127.0.0.1", help="Server Host")
    parser.add_argument("--port", type=int, default=8080, help="Server Port")
    parser.add_argument("-c", "--concurrency", type=int, default=200, help="Number of concurrent workers")
    parser.add_argument("-r", "--requests", type=int, default=1000000, help="Total number of requests")
    parser.add_argument("-p", "--pipeline", type=int, default=50, help="Pipeline depth (1 = Ping-Pong)")
    args = parser.parse_args()
    
    asyncio.run(run_benchmark(args))
