import psutil

avail_memory = psutil.virtual_memory().available

avail_memory_mb = avail_memory / (1024 ** 2)

print(f"Available Memory: {avail_memory_mb} MB")