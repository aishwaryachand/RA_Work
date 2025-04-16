from pytubefix import YouTube
from multiprocessing import get_context
import time
import os



# ---- CONFIGURATION ----

BATCH_SIZE = 200
WORKERS = min(10, os.cpu_count())
VIDEO_ID_FILE = "video_ids_to_download.txt"
DOWNLOAD_DIR = "final_download"
FAILED_LOG = "failed_downloads.txt"

# ---- DOWNLOAD FUNCTION WITH ERROR LOGGING ----
def download_video(video_id):
    url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        yt = YouTube(
            url,
            use_po_token=True,
            # po_token=PO_TOKEN,
            # visitor_data=VISITOR_DATA,
            client="WEB"
        )
        print(f" Downloading: {video_id}")
        ys = yt.streams.get_highest_resolution()
        ys.download(output_path=DOWNLOAD_DIR, filename=f"{video_id}.mp4")
        print(f" Downloaded: {video_id}")
    except Exception as e:
        print(f"Failed to download video ID: {video_id} | Reason: {e}")
        with open(FAILED_LOG, "a") as f:
            f.write(video_id + "\n")

# ---- BATCH WRAPPER ----
def download_batch(video_ids):
    for video_id in video_ids:
        download_video(video_id)
        time.sleep(1.5)  # Throttle to reduce risk of 429

# ---- MAIN FUNCTION ----
def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    # Clean previous failed log
    if os.path.exists(FAILED_LOG):
        os.remove(FAILED_LOG)

    # Read and clean video IDs
    with open(VIDEO_ID_FILE, "r") as f:
        video_ids = [line.strip() for line in f if line.strip()]

    # Create chunks for parallel processing
    chunks = [video_ids[i:i + BATCH_SIZE] for i in range(0, len(video_ids), BATCH_SIZE)]

    print(f"Total Videos: {len(video_ids)} |  Batches: {len(chunks)} |  Workers: {WORKERS}")

    # Launch multiprocessing
    ctx = get_context("spawn")
    with ctx.Pool(processes=WORKERS) as pool:
        pool.map(download_batch, chunks)

if __name__ == "__main__":
    main()
