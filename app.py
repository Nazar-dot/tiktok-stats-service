from flask import Flask, request, jsonify
import os
import uuid
import threading
import requests

app = Flask(__name__)

# In-memory store: jobId -> {"ready": bool, "results": [ ... ]}
jobs = {}

BRIGHTDATA_TOKEN = os.getenv("BRIGHTDATA_TOKEN")
DATASET_ID       = "gd_lu702nij2f790tmv9h"
ENDPOINT         = "https://api.brightdata.com/datasets/v3/scrape"

def fetch_stats(job_id, urls):
    payload = {"input": [{"url": url} for url in urls]}
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {BRIGHTDATA_TOKEN}"
    }
    try:
        resp = requests.post(
            f"{ENDPOINT}?dataset_id={DATASET_ID}&format=json",
            json=payload, headers=headers, timeout=60
        )
        data = resp.json() if resp.ok else []
    except Exception:
        data = []

    # Build fallback: if data list shorter than urls, pad with None
    results = []
    for i, url in enumerate(urls):
        rec = data[i] if i < len(data) and isinstance(data, list) else {}
        results.append({
            "url":        url,
            "play_count": rec.get("play_count", None),
            "digg_count": rec.get("digg_count", None)
        })

    jobs[job_id]["results"] = results
    jobs[job_id]["ready"]   = True

@app.route("/start", methods=["POST"])
def start_job():
    body = request.get_json()
    urls = body.get("urls", [])
    if not isinstance(urls, list) or not urls:
        return jsonify(error="No URLs provided"), 400

    job_id = str(uuid.uuid4())
    jobs[job_id] = {"ready": False, "results": []}

    # Launch background thread to fetch stats
    thread = threading.Thread(target=fetch_stats, args=(job_id, urls))
    thread.daemon = True
    thread.start()

    return jsonify(jobId=job_id), 200

@app.route("/status", methods=["GET"])
def check_status():
    job_id = request.args.get("jobId")
    job = jobs.get(job_id)
    if not job:
        return jsonify(error="Unknown jobId"), 404

    if not job["ready"]:
        # not ready yet
        return jsonify(status="pending"), 202

    # return completed results
    return jsonify(results=job["results"]), 200

if __name__ == "__main__":
    # Run on port 10000 or override via $PORT
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
