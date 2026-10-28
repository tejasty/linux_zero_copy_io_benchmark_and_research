import subprocess
import time
import os
import csv
import re

endpoints = [
    ("Traditional_4K", "http://localhost:8080/traditional/4k"),
    ("Sendfile_4K", "http://localhost:8080/zerocopy/4k"),
    ("IOUring_4K", "http://localhost:8080/iouring/4k"),
    ("Traditional_50M", "http://localhost:8080/traditional/50m"),
    ("Sendfile_50M", "http://localhost:8080/zerocopy/50m"),
    ("IOUring_50M", "http://localhost:8080/iouring/50m"),
]

results = []

def start_server():
    print("[*] Starting C Zero-Copy Research Engine...")
    proc = subprocess.Popen(["./zero_copy_server"])
    time.sleep(2)
    return proc

server_proc = start_server()

try:
    for name, url in endpoints:
        print(f"\n[+] Running Benchmark for: {name} ({url})")

        # Check if server process died, restart if needed
        if server_proc.poll() is not None:
            print("[!] Server process crashed. Restarting...")
            server_proc = start_server()

        time.sleep(1) 

        # Attach perf stat for 10 seconds
        perf_cmd = [
            "sudo", "perf", "stat", "-e",
            "context-switches,cpu-migrations,page-faults,cache-misses",
            "-p", str(server_proc.pid), "--", "sleep", "10"
        ]
        perf_proc = subprocess.Popen(perf_cmd, stderr=subprocess.PIPE, text=True)

        # Run wrk load benchmark
        wrk_cmd = ["wrk", "-t4", "-c50", "-d10s", "--latency", url]
        
        try:
            wrk_output = subprocess.check_output(wrk_cmd, text=True)
            _, perf_err = perf_proc.communicate()

            rps_match = re.search(r"Requests/sec:\s+([\d.]+)", wrk_output)
            rps = float(rps_match.group(1)) if rps_match else 0.0

            cs_match = re.search(r"([\d,]+)\s+context-switches", perf_err)
            cm_match = re.search(r"([\d,]+)\s+cache-misses", perf_err)

            context_switches = cs_match.group(1).replace(",", "") if cs_match else "0"
            cache_misses = cm_match.group(1).replace(",", "") if cm_match else "0"

            print(f"    - Requests/sec: {rps}")
            print(f"    - Context Switches: {context_switches}")
            print(f"    - Cache Misses: {cache_misses}")

            results.append({
                "Experiment": name,
                "RPS": rps,
                "Context_Switches": context_switches,
                "Cache_Misses": cache_misses
            })
        except subprocess.CalledProcessError as e:
            print(f"[-] Benchmark for {name} failed or server dropped connection.")

finally:
    print("\n[*] Terminating Server...")
    if server_proc.poll() is None:
        server_proc.terminate()

with open("results.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["Experiment", "RPS", "Context_Switches", "Cache_Misses"])
    writer.writeheader()
    writer.writerows(results)

print("\n[+] Success! Data collected and saved to 'results.csv'.")