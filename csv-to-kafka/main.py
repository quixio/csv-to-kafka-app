import os
import csv
import io

from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from quixstreams import Application
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()


@app.get("/", response_class=HTMLResponse)
async def index():
    with open(os.path.join("static", "index.html")) as f:
        return f.read()


@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        return {"status": "error", "message": "Only CSV files are allowed"}

    topic_name = os.environ.get("output", "csv-data")

    quix_app = Application(
        consumer_group="csv-uploader",
        auto_create_topics=True,
    )
    topic = quix_app.topic(name=topic_name, value_serializer="json")
    key = os.path.splitext(file.filename)[0]

    sent = 0
    with quix_app.get_producer() as producer:
        # Stream the file line by line to avoid loading everything into memory
        reader = csv.DictReader(io.TextIOWrapper(file.file, encoding="utf-8"))
        for row in reader:
            value = {}
            for k, v in row.items():
                try:
                    value[k] = float(v)
                except (ValueError, TypeError):
                    value[k] = v

            message = topic.serialize(key=key, value=value)
            producer.produce(
                topic=topic.name,
                key=message.key,
                value=message.value,
            )
            sent += 1

    if sent == 0:
        return {"status": "error", "message": "CSV file is empty"}

    return {"status": "ok", "rows_sent": sent, "topic": topic_name, "filename": file.filename}
