import os
import psutil
import pyarrow.parquet as pq
import requests
from tqdm import tqdm
from io import BytesIO

# Function to download image from URL
def download_image(url, filename):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()

        file_ext = os.path.splitext(url)[1]
        if file_ext.lower() not in [".jpg", ".jpeg", ".png", ".gif"]:
            file_ext = ".jpg"
        # Save the image to a file
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

    except requests.exceptions.RequestException as e:
        print(f"Failed to download image from URL: {url}")
        print(f"Error: {e}")

# Function to monitor CPU usage
def monitor_cpu():
    cpu_percent = psutil.cpu_percent(interval=1)
    print(f'CPU Usage: {cpu_percent}%')

# Function to monitor network usage
def monitor_network():
    network_usage = psutil.net_io_counters()
    print(f'Network Usage (bytes sent/received): {network_usage.bytes_sent}/{network_usage.bytes_recv}')

def main():
    # Get the first 10,000 URLs
    urls = pq.read_table("links.parquet", columns=["URL"])["URL"][:10000].to_numpy()

    # Create a directory to store the images
    os.makedirs("images", exist_ok=True)

    for i, url in enumerate(tqdm(urls, desc="Downloading images")):
        filename = f"images/image_{i}.jpg"
        download_image(url, filename)
        monitor_cpu()
        monitor_network()

if __name__ == "__main__":
    main()
