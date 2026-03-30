import os
import csv
import io
import json

from fastapi import FastAPI, Request
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
async def upload_csv(request: Request):
    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" not in content_type:
        return {"status": "error", "message": "Expected multipart/form-data"}

    # Get file size from content-length header (approximate, includes multipart overhead)
    content_length = int(request.headers.get("content-length", 0))

    async def stream_progress():
        topic_name = os.environ.get("output", "csv-data")

        quix_app = Application(
            consumer_group="csv-uploader",
            auto_create_topics=True,
        )
        topic = quix_app.topic(name=topic_name, value_serializer="json")

        # Parse the streaming body chunk by chunk
        buffer = ""
        headers = None
        key = None
        sent = 0
        batch = []
        bytes_received = 0
        filename = "upload"

        # Read the multipart boundary
        boundary = content_type.split("boundary=")[-1].strip()
        in_file = False
        past_headers = False
        producer = quix_app.get_producer()

        async for chunk in request.stream():
            bytes_received += len(chunk)
            text = chunk.decode("utf-8", errors="replace")

            if not in_file:
                # Look for the file content start (after multipart headers)
                if f"--{boundary}" in text:
                    in_file = True
                    # Extract filename from Content-Disposition
                    if 'filename="' in text:
                        fn_start = text.index('filename="') + 10
                        fn_end = text.index('"', fn_start)
                        filename = text[fn_start:fn_end]
                        key = os.path.splitext(filename)[0]

                    # Skip past the multipart headers (empty line after headers)
                    parts = text.split("\r\n\r\n", 1)
                    if len(parts) > 1:
                        past_headers = True
                        text = parts[1]
                        # Remove trailing boundary if present
                        if f"\r\n--{boundary}" in text:
                            text = text[:text.index(f"\r\n--{boundary}")]
                    else:
                        continue
            else:
                # Remove trailing boundary if this is the last chunk
                if f"\r\n--{boundary}" in text:
                    text = text[:text.index(f"\r\n--{boundary}")]

            if not past_headers:
                if "\r\n\r\n" in text:
                    past_headers = True
                    text = text.split("\r\n\r\n", 1)[1]
                else:
                    continue

            buffer += text
            lines = buffer.split("\n")
            buffer = lines.pop()  # keep incomplete last line

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                if headers is None:
                    headers = [h.strip() for h in line.split(",")]
                    continue

                # Parse CSV row
                reader = csv.reader(io.StringIO(line))
                try:
                    values = next(reader)
                except StopIteration:
                    continue

                if len(values) != len(headers):
                    continue

                row = {}
                for h, v in zip(headers, values):
                    try:
                        row[h] = float(v)
                    except (ValueError, TypeError):
                        row[h] = v

                batch.append(row)

                if len(batch) >= BATCH_SIZE:
                    for r in batch:
                        message = topic.serialize(key=key, value=r)
                        producer.produce(
                            topic=topic.name,
                            key=message.key,
                            value=message.value,
                        )
                    producer.flush()
                    sent += len(batch)
                    batch = []
                    pct = min(99, round(bytes_received / content_length * 100)) if content_length else 0
                    yield f"data: {json.dumps({'status': 'progress', 'rows_sent': sent, 'percent': pct})}\n\n"

        # Process remaining buffer
        if buffer.strip():
            if headers is not None:
                reader = csv.reader(io.StringIO(buffer.strip()))
                try:
                    values = next(reader)
                    if len(values) == len(headers):
                        row = {}
                        for h, v in zip(headers, values):
                            try:
                                row[h] = float(v)
                            except (ValueError, TypeError):
                                row[h] = v
                        batch.append(row)
                except StopIteration:
                    pass

        # Send remaining batch
        if batch:
            for r in batch:
                message = topic.serialize(key=key, value=r)
                producer.produce(
                    topic=topic.name,
                    key=message.key,
                    value=message.value,
                )
            producer.flush()
            sent += len(batch)

        producer.flush()

        yield f"data: {json.dumps({'status': 'done', 'rows_sent': sent, 'percent': 100, 'topic': topic_name, 'filename': filename})}\n\n"

    return StreamingResponse(
        stream_progress(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
