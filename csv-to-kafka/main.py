import os
import csv
import io
import json

from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse, StreamingResponse
from quixstreams import Application
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

BATCH_SIZE = int(os.environ.get("batch_size", "10000"))


@app.get("/", response_class=HTMLResponse)
async def index():
    with open(os.path.join("static", "index.html")) as f:
        return f.read()


@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        return {"status": "error", "message": "Only CSV files are allowed"}

    async def stream_progress():
        topic_name = os.environ.get("output", "csv-data")

        quix_app = Application(
            consumer_group="csv-uploader",
            auto_create_topics=True,
        )
        topic = quix_app.topic(name=topic_name, value_serializer="json")
        key = os.path.splitext(file.filename)[0]

        sent = 0
        with quix_app.get_producer() as producer:
            reader = csv.DictReader(io.TextIOWrapper(file.file, encoding="utf-8"))
            batch = []
            for row in reader:
                value = {}
                for k, v in row.items():
                    try:
                        value[k] = float(v)
                    except (ValueError, TypeError):
                        value[k] = v
                batch.append(value)

                if len(batch) >= BATCH_SIZE:
                    for v in batch:
                        message = topic.serialize(key=key, value=v)
                        producer.produce(
                            topic=topic.name,
                            key=message.key,
                            value=message.value,
                        )
                    producer.flush()
                    sent += len(batch)
                    batch = []
                    yield f"data: {json.dumps({'status': 'progress', 'rows_sent': sent})}\n\n"

            # Send remaining rows
            if batch:
                for v in batch:
                    message = topic.serialize(key=key, value=v)
                    producer.produce(
                        topic=topic.name,
                        key=message.key,
                        value=message.value,
                    )
                producer.flush()
                sent += len(batch)

        yield f"data: {json.dumps({'status': 'done', 'rows_sent': sent, 'topic': topic_name, 'filename': file.filename})}\n\n"

    return StreamingResponse(stream_progress(), media_type="text/event-stream")
