#!/usr/bin/env python3
import subprocess, re, csv, itertools, concurrent.futures

HOST_FILE = "hosts.txt"   # 你的 IP 列表文件
COUNT = 3                 # 每个目标 ping 次数
CONCURRENCY = 32          # 并发线程数

def load_public_hosts(path):
    hosts = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "-private" in line:
                continue
            ip = line.split()[0]
            hosts.append(ip)
    return hosts

def ping(ip):
    try:
        output = subprocess.check_output(
            ["ping", "-c", str(COUNT), "-q", ip],
            stderr=subprocess.STDOUT, text=True, timeout=10
        )
        m = re.search(r"= ([\d\.]+)/([\d\.]+)/([\d\.]+)/", output)
        if m:
            avg, mx = float(m.group(2)), float(m.group(3))
            return avg, mx
    except Exception:
        pass
    return float("nan"), float("nan")

def measure_pair(src, dst):
    if src == dst:
        return (src, dst, None, None)
    avg, mx = ping(dst)
    print(f"{src} → {dst}: avg={avg:.3f} ms, max={mx:.3f} ms")
    return (src, dst, avg, mx)

def main():
    hosts = load_public_hosts(HOST_FILE)
    print(f"🌍 Public nodes detected: {len(hosts)}")
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        tasks = [(s, d) for s, d in itertools.product(hosts, hosts)]
        for src, dst, avg, mx in ex.map(lambda p: measure_pair(*p), tasks):
            if src != dst:
                results.append((src, dst, avg, mx))

    with open("rtt_results.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["from", "to", "avg_ms", "max_ms"])
        w.writerows(results)

    valid = [r for r in results if not (r[2] != r[2])]
    if valid:
        avg_all = sum(r[2] for r in valid) / len(valid)
        mx_all = max(r[3] for r in valid)
        print(f"\n✅ Global average RTT: {avg_all:.3f} ms")
        print(f"✅ Global max RTT:     {mx_all:.3f} ms")
    print("📄 Saved to rtt_results.csv")

if __name__ == "__main__":
    main()