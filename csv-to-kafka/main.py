import os
import csv
import io
import json
from itertools import islice

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

    # Get file size for progress tracking (no need to read the whole file)
    file.file.seek(0, 2)  # seek to end
    file_size = file.file.tell()
    file.file.seek(0)  # seek back to start

    if file_size == 0:
        return {"status": "error", "message": "CSV file is empty"}

    def stream_progress():
        topic_name = os.environ.get("output", "csv-data")

        quix_app = Application(
            consumer_group="csv-uploader",
            auto_create_topics=True,
        )
        topic = quix_app.topic(name=topic_name, value_serializer="json")
        key = os.path.splitext(file.filename)[0]

        reader = csv.DictReader(io.TextIOWrapper(file.file, encoding="utf-8"))
        sent = 0

        with quix_app.get_producer() as producer:
            while True:
                batch = list(islice(reader, BATCH_SIZE))
                if not batch:
                    break

                for row in batch:
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

                producer.flush()
                    
                sent += len(batch)
                bytes_read = file.file.tell()
                pct = min(99, round(bytes_read / file_size * 100))
                yield f"data: {json.dumps({'status': 'progress', 'rows_sent': sent, 'percent': pct})}\n\n"

        yield f"data: {json.dumps({'status': 'done', 'rows_sent': sent, 'percent': 100, 'topic': topic_name, 'filename': file.filename})}\n\n"

    return StreamingResponse(stream_progress(), media_type="text/event-stream")
