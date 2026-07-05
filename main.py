import asyncio
import io
import os
import shutil
import struct
import tempfile
import time
import wave
from collections import deque
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse

SERVER_NAME = "Vidloq Stream Server V6.3 README Update"
SERVER_HOST = os.environ.get("SERVER_HOST", "sparqm.com")
AUDIO_TCP_HOST = "0.0.0.0"
AUDIO_TCP_PORT = 8001
SAMPLE_RATE = 16000
CHANNELS = 1
BITS_PER_SAMPLE = 16
BYTES_PER_SAMPLE = 2
MAX_AUDIO_SECONDS = 60
MAX_AUDIO_BYTES = SAMPLE_RATE * BYTES_PER_SAMPLE * MAX_AUDIO_SECONDS
AUDIO_FRAME_MAGIC = b"AUD2"
AUDIO_HEADER_BYTES = 24
AUDIO_HEADER_STRUCT = "<IHHHII"
AUDIO_HEADER_STRUCT_BYTES = struct.calcsize(AUDIO_HEADER_STRUCT)
VIDEO_TIMEOUT_SECONDS = 10
AUDIO_TIMEOUT_SECONDS = 10

api_schema_key = "open" + "api_url"
app = FastAPI(title=SERVER_NAME, docs_url=None, redoc_url=None, **{api_schema_key: None})

state_lock = asyncio.Lock()
latest_jpeg: Optional[bytes] = None
latest_video_time: Optional[float] = None
latest_audio_time: Optional[float] = None
video_frame_count = 0
audio_frame_count = 0
audio_tcp_frame_count = 0
audio_http_rejected_count = 0
audio_sample_count = 0
audio_bytes_total = 0
audio_gap_samples = 0
audio_bad_frames = 0
audio_connections = 0
video_ws_connected = False
audio_tcp_connected = False
last_audio_seq: Optional[int] = None
last_error: Optional[str] = None
audio_ring = deque()
audio_ring_size = 0
audio_level_peak = 0
stream_audio_queues = []

minute_window_start = time.time()
minute_audio_frames = 0
minute_audio_bytes = 0
minute_audio_samples = 0
minute_audio_peak = 0
minute_audio_bad_frames = 0
minute_audio_connections = 0
minute_audio_disconnects = 0
minute_http_audio_rejected = 0
minute_video_frames = 0
minute_video_bytes = 0
minute_video_last_size = 0
minute_video_connections = 0
minute_video_disconnects = 0


def now() -> float:
    return time.time()


def pcm16_peak(data: bytes) -> int:
    peak = 0
    limit = len(data) - (len(data) % 2)
    for i in range(0, limit, 2):
        v = int.from_bytes(data[i:i + 2], "little", signed=True)
        av = abs(v)
        if av > peak:
            peak = av
    return peak


def append_audio(pcm: bytes, seq: Optional[int] = None, frame_samples: Optional[int] = None):
    global audio_ring_size, audio_frame_count, audio_tcp_frame_count, audio_sample_count, audio_bytes_total
    global audio_gap_samples, audio_level_peak, latest_audio_time, last_audio_seq
    global minute_audio_frames, minute_audio_bytes, minute_audio_samples, minute_audio_peak
    if not pcm:
        return

    insert_silence = b""
    if seq is not None and last_audio_seq is not None and seq > last_audio_seq + 1:
        missing = seq - last_audio_seq - 1
        samples_per_frame = frame_samples or (len(pcm) // BYTES_PER_SAMPLE)
        gap = missing * samples_per_frame
        audio_gap_samples += gap
        insert_silence = b"\x00\x00" * gap
    if seq is not None:
        last_audio_seq = seq

    data = insert_silence + pcm
    audio_ring.append(data)
    audio_ring_size += len(data)
    while audio_ring_size > MAX_AUDIO_BYTES and audio_ring:
        removed = audio_ring.popleft()
        audio_ring_size -= len(removed)

    audio_frame_count += 1
    audio_tcp_frame_count += 1
    samples_in_pcm = len(pcm) // BYTES_PER_SAMPLE
    audio_sample_count += samples_in_pcm
    audio_bytes_total += len(pcm)
    audio_level_peak = pcm16_peak(pcm)

    minute_audio_frames += 1
    minute_audio_bytes += len(pcm)
    minute_audio_samples += samples_in_pcm
    if audio_level_peak > minute_audio_peak:
        minute_audio_peak = audio_level_peak
    latest_audio_time = now()

    stale = []
    for q in stream_audio_queues:
        try:
            q.put_nowait(pcm)
        except asyncio.QueueFull:
            stale.append(q)
    for q in stale:
        try:
            stream_audio_queues.remove(q)
        except ValueError:
            pass


def current_audio_bytes() -> bytes:
    return b"".join(audio_ring)


def wav_response_bytes(pcm: bytes) -> bytes:
    out = io.BytesIO()
    with wave.open(out, "wb") as wf:
        wf.setnchannels(CHANNELS)
        wf.setsampwidth(BYTES_PER_SAMPLE)
        wf.setframerate(SAMPLE_RATE)
        wf.writeframes(pcm)
    return out.getvalue()


async def handle_audio_tcp(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    global audio_tcp_connected, audio_connections, last_error, audio_bad_frames
    global minute_audio_connections, minute_audio_disconnects, minute_audio_bad_frames
    audio_connections += 1
    minute_audio_connections += 1
    audio_tcp_connected = True
    try:
        while True:
            header = await reader.readexactly(AUDIO_HEADER_BYTES)
            if header[:4] != AUDIO_FRAME_MAGIC:
                audio_bad_frames += 1
                minute_audio_bad_frames += 1
                last_error = "bad audio TCP magic"
                break

            seq, sample_rate, channels, bits, sample_count, pcm_len = struct.unpack(
                AUDIO_HEADER_STRUCT, header[4:4 + AUDIO_HEADER_STRUCT_BYTES]
            )
            if sample_rate != SAMPLE_RATE or channels != CHANNELS or bits != BITS_PER_SAMPLE:
                audio_bad_frames += 1
                minute_audio_bad_frames += 1
                last_error = f"bad audio format sr={sample_rate} ch={channels} bits={bits}"
                break
            if pcm_len <= 0 or pcm_len > 32768:
                audio_bad_frames += 1
                minute_audio_bad_frames += 1
                last_error = f"bad pcm length {pcm_len}"
                break

            pcm = await reader.readexactly(pcm_len)
            async with state_lock:
                append_audio(pcm, seq=seq, frame_samples=sample_count)
    except asyncio.IncompleteReadError:
        pass
    except Exception as e:
        last_error = f"audio tcp error: {e}"
    finally:
        audio_tcp_connected = False
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        minute_audio_disconnects += 1


async def minute_report_task():
    global minute_window_start
    global minute_audio_frames, minute_audio_bytes, minute_audio_samples, minute_audio_peak
    global minute_audio_bad_frames, minute_audio_connections, minute_audio_disconnects, minute_http_audio_rejected
    global minute_video_frames, minute_video_bytes, minute_video_last_size
    global minute_video_connections, minute_video_disconnects
    while True:
        await asyncio.sleep(60)
        elapsed = max(0.001, now() - minute_window_start)
        audio_seconds = minute_audio_samples / SAMPLE_RATE if SAMPLE_RATE else 0
        expected_audio_seconds = elapsed if minute_audio_frames > 0 else 0
        coverage_percent = 0 if expected_audio_seconds == 0 else min(100.0, (audio_seconds / expected_audio_seconds) * 100.0)
        print(
            "minute report "
            f"window_seconds={elapsed:.1f} "
            f"audio_chunks={minute_audio_frames} "
            f"audio_seconds={audio_seconds:.2f} "
            f"audio_coverage_percent={coverage_percent:.1f} "
            f"audio_bytes={minute_audio_bytes} "
            f"audio_peak={minute_audio_peak} "
            f"audio_bad_frames={minute_audio_bad_frames} "
            f"audio_tcp_connections={minute_audio_connections} "
            f"audio_tcp_disconnects={minute_audio_disconnects} "
            f"audio_http_rejected={minute_http_audio_rejected} "
            f"video_images={minute_video_frames} "
            f"video_bytes={minute_video_bytes} "
            f"latest_video_size={minute_video_last_size} "
            f"video_connections={minute_video_connections} "
            f"video_disconnects={minute_video_disconnects} "
            f"total_audio_frames={audio_frame_count} "
            f"total_video_frames={video_frame_count} "
            f"last_error={last_error}",
            flush=True,
        )
        minute_window_start = time.time()
        minute_audio_frames = 0
        minute_audio_bytes = 0
        minute_audio_samples = 0
        minute_audio_peak = 0
        minute_audio_bad_frames = 0
        minute_audio_connections = 0
        minute_audio_disconnects = 0
        minute_http_audio_rejected = 0
        minute_video_frames = 0
        minute_video_bytes = 0
        minute_video_last_size = 0
        minute_video_connections = 0
        minute_video_disconnects = 0


@app.on_event("startup")
async def startup_event():
    server = await asyncio.start_server(handle_audio_tcp, AUDIO_TCP_HOST, AUDIO_TCP_PORT)
    app.state.audio_tcp_server = server
    print(f"=== {SERVER_NAME} ===", flush=True)
    print(f"HTTP server: 0.0.0.0:8000", flush=True)
    print(f"Raw continuous audio TCP ingest: {AUDIO_TCP_HOST}:{AUDIO_TCP_PORT}", flush=True)
    print("Terminal output is condensed to one combined report per minute.", flush=True)
    asyncio.create_task(minute_report_task())


@app.on_event("shutdown")
async def shutdown_event():
    server = getattr(app.state, "audio_tcp_server", None)
    if server:
        server.close()
        await server.wait_closed()


@app.websocket("/device/video")
async def device_video(websocket: WebSocket):
    global latest_jpeg, latest_video_time, video_frame_count, video_ws_connected
    global minute_video_connections, minute_video_disconnects
    global minute_video_frames, minute_video_bytes, minute_video_last_size
    await websocket.accept()
    video_ws_connected = True
    minute_video_connections += 1
    try:
        while True:
            data = await websocket.receive_bytes()
            if data.startswith(b"\xff\xd8"):
                async with state_lock:
                    latest_jpeg = data
                    latest_video_time = now()
                    video_frame_count += 1
                    minute_video_frames += 1
                    minute_video_bytes += len(data)
                    minute_video_last_size = len(data)
    except WebSocketDisconnect:
        pass
    finally:
        video_ws_connected = False
        minute_video_disconnects += 1


@app.post("/device/audio")
async def device_audio_post(request: Request):
    global audio_http_rejected_count, minute_http_audio_rejected, last_error
    await request.body()
    audio_http_rejected_count += 1
    minute_http_audio_rejected += 1
    last_error = "HTTP audio rejected; use Heltec TCP continuous audio firmware on port 8001"
    return JSONResponse(
        {
            "ok": False,
            "reason": "HTTP audio chunks are disabled because they created stitched/garbled audio. Use the Heltec raw TCP audio firmware on port 8001.",
            "required_audio_path": "raw TCP to HOST:8001",
        },
        status_code=409,
    )


@app.get("/health")
async def health():
    t = now()
    audio_seconds = audio_sample_count / SAMPLE_RATE if SAMPLE_RATE else 0
    return {
        "server": SERVER_NAME,
        "http_port": 8000,
        "audio_tcp_port": AUDIO_TCP_PORT,
        "video_frame_count": video_frame_count,
        "audio_frame_count": audio_frame_count,
        "audio_tcp_frame_count": audio_tcp_frame_count,
        "audio_http_rejected_count": audio_http_rejected_count,
        "audio_sample_count": audio_sample_count,
        "audio_seconds_received": round(audio_seconds, 3),
        "audio_bytes_total": audio_bytes_total,
        "audio_gap_samples_inserted": audio_gap_samples,
        "audio_bad_frames": audio_bad_frames,
        "has_video_frame": latest_jpeg is not None,
        "has_audio": audio_ring_size > 0,
        "video_ws": video_ws_connected,
        "audio_tcp_connected": audio_tcp_connected,
        "seconds_since_video": None if latest_video_time is None else round(t - latest_video_time, 3),
        "seconds_since_audio": None if latest_audio_time is None else round(t - latest_audio_time, 3),
        "audio_peak": audio_level_peak,
        "last_error": last_error,
        "current_minute_audio_chunks": minute_audio_frames,
        "current_minute_video_images": minute_video_frames,
        "valid_urls": valid_url_map(),
    }


def valid_url_map():
    return {
        "home": "/",
        "preview": "/preview",
        "health": "/health",
        "latest_jpg": "/debug/latest.jpg",
        "mjpeg": "/debug/mjpeg",
        "audio_level": "/debug/audio_level",
        "audio_wav": "/debug/audio.wav",
        "continuous_audio_wav": "/debug/audio_continuous.wav",
        "audio_raw_pcm": "/debug/audio_raw.pcm",
        "stream_test": "/stream.ts",
        "reset_post": "/admin/reset",
        "video_websocket": "ws://HOST:8000/device/video",
        "audio_raw_tcp": "HOST:8001",
        "disabled_audio_http_post": "http://HOST:8000/device/audio",
        "docs": "/docs",
        "videos_json": "/videos.json",
    }


URL_ROWS = [
    ("/", "GET", "Main Vidloq home page with dark theme, endpoint list, and test order. No request body is needed."),
    ("/docs", "GET", "Interactive Vidloq documentation. Each pull-down section includes request details, a Try Request button when browser testing is possible, and an inline response panel."),
    ("/preview", "GET", "Same page as the main home page. No request body is needed."),
    ("/health", "GET", "Returns server status, counters, device connection flags, audio/video timing, and valid URL metadata. No request body is needed."),
    ("/debug/latest.jpg", "GET", "Returns the latest JPEG image received from the ESP32-CAM. No request body is needed."),
    ("/debug/mjpeg", "GET", "Browser-viewable MJPEG debug feed built from the latest ESP32-CAM image. No request body is needed."),
    ("/debug/audio_level", "GET", "Returns microphone availability, peak level, audio frame counters, and buffer duration. No request body is needed."),
    ("/debug/audio.wav", "GET", "Returns the current rolling audio buffer as a WAV file. No request body is needed."),
    ("/debug/audio_continuous.wav", "GET", "Returns the continuous TCP audio buffer as a WAV file. No request body is needed."),
    ("/debug/audio_raw.pcm", "GET", "Returns raw 16-bit mono PCM audio for diagnostics. No request body is needed."),
    ("/stream.ts", "GET", "MPEG-TS stream test for VLC or Android playback. No request body is needed. Test only after latest.jpg and audio_continuous.wav work."),
    ("/admin/reset", "POST", "Clears current audio/video buffers and counters. Requires an HTTP POST request; no request body is required."),
    ("ws://HOST:8000/device/video", "WEBSOCKET", "ESP32-CAM video ingest endpoint. Requires JPEG bytes sent over a WebSocket connection. Browser docs can connect, but real testing requires ESP32-CAM firmware."),
    ("HOST:8001", "RAW TCP", "Heltec continuous microphone ingest endpoint. Requires Vidloq AUD2 framed 16-bit mono PCM over a raw TCP socket; not an HTTP URL."),
    ("http://HOST:8000/device/audio", "POST disabled", "Old HTTP audio ingest endpoint. Requires audio data if used, but this server rejects it because TCP audio is the supported path."),
    ("/videos.json", "GET", "Machine-readable Vidloq endpoint list for tools, documentation, and clients. No request body is needed."),
]


def url_to_display(path: str) -> str:
    if path.startswith("ws://") or path.startswith("http://") or path.startswith("HOST:"):
        return path.replace("HOST", SERVER_HOST)
    return f"http://{SERVER_HOST}:8000{path}"


@app.get("/videos.json")
async def videos_json():
    return {
        "name": SERVER_NAME,
        "host": SERVER_HOST,
        "http_port": 8000,
        "audio_tcp_port": AUDIO_TCP_PORT,
        "endpoints": [
            {"url": url_to_display(path), "method": method, "description": description}
            for path, method, description in URL_ROWS
        ],
    }


@app.get("/docs")
async def vidloq_docs():
    docs_rows = []
    docs_url_rows = [row for row in URL_ROWS if row[0] != "/docs"]
    for idx, (path, method, description) in enumerate(docs_url_rows):
        full_url = url_to_display(path)
        safe_id = f"endpoint_{idx}"
        can_fetch = method in ("GET", "POST") and path.startswith("/") and method != "POST disabled"
        body_hint = "No request body is needed."
        if path == "/admin/reset":
            body_hint = "POST request. No request body is required."
        elif method == "WEBSOCKET":
            body_hint = "Requires binary JPEG bytes over WebSocket from the ESP32-CAM firmware."
        elif method == "RAW TCP":
            body_hint = "Requires Vidloq AUD2 framed 16-bit mono PCM from the Heltec firmware."
        elif method == "POST disabled":
            body_hint = "Disabled compatibility endpoint. It accepts a POST request but returns a rejection message."
        request_url = path if path.startswith("/") else full_url
        if method == "POST disabled":
            request_url = "/device/audio"
        button = ""
        body_box = ""
        if can_fetch:
            button = f'<button type="button" onclick="tryRequest(\'{safe_id}\', \'{method}\', \'{request_url}\')">Try Request</button>'
        elif method == "WEBSOCKET":
            button = f'<button type="button" onclick="tryWebSocket(\'{safe_id}\', \'/device/video\')">Try WebSocket Connect</button>'
        else:
            button = '<button type="button" disabled>Manual device test required</button>'
        if method == "POST disabled":
            body_box = f'<textarea id="body_{safe_id}" placeholder="Optional test payload"></textarea>'
            button = f'<button type="button" onclick="tryPostDisabled(\'{safe_id}\', \'{request_url}\')">Try POST</button>'
        docs_rows.append(
            f'<details class="endpoint">'
            f'<summary><span class="method">{method}</span> <code>{full_url}</code></summary>'
            f'<div class="endpoint-body">'
            f'<p>{description}</p>'
            f'<p><strong>Data requirement:</strong> {body_hint}</p>'
            f'{body_box}'
            f'<div class="actions">{button}</div>'
            f'<pre id="response_{safe_id}">Response will appear here.</pre>'
            f'</div>'
            f'</details>'
        )
    return HTMLResponse(
        f"""
        <html>
        <head>
          <title>Vidloq Interactive Docs</title>
          <style>
            body {{ font-family: Arial, sans-serif; margin: 24px; line-height: 1.45; background: #0f1117; color: #e8eaf0; }}
            a {{ color: #7cc7ff; }}
            code {{ color: #9ee6ff; word-break: break-all; }}
            .top {{ padding: 16px; border: 1px solid #30384a; background: #151924; border-radius: 10px; margin-bottom: 18px; }}
            details.endpoint {{ border: 1px solid #30384a; background: #151924; border-radius: 10px; margin: 10px 0; }}
            summary {{ cursor: pointer; padding: 12px; font-weight: bold; }}
            .endpoint-body {{ padding: 0 12px 12px 12px; }}
            .method {{ display: inline-block; min-width: 92px; color: #ffffff; background: #28324a; border-radius: 6px; padding: 3px 6px; margin-right: 8px; text-align: center; }}
            button {{ background: #2d7ff9; color: white; border: 0; padding: 8px 12px; border-radius: 6px; cursor: pointer; }}
            button:disabled {{ background: #4a4f5f; cursor: not-allowed; }}
            textarea {{ width: 100%; height: 90px; background: #0f1117; color: #e8eaf0; border: 1px solid #30384a; border-radius: 6px; padding: 8px; }}
            pre {{ background: #090b10; border: 1px solid #30384a; border-radius: 8px; padding: 10px; overflow: auto; min-height: 48px; white-space: pre-wrap; }}
            .footer {{ margin-top: 32px; color: #aab2c5; font-size: 14px; }}
          </style>
        </head>
        <body>
          <h1>Vidloq Interactive Docs</h1>
          <div class="top">
            <p>Server host: <strong>{SERVER_HOST}</strong></p>
            <p>This page lets you open each endpoint section, send supported browser requests, and see the response inside the same pull-down section.</p>
            <p><a href="http://{SERVER_HOST}:8000/">Back to Vidloq home page</a></p>
          </div>
          {''.join(docs_rows)}
          <div class="footer">Designed by Harold Paulino</div>
          <script>
            async function tryRequest(id, method, url) {{
              const out = document.getElementById('response_' + id);
              out.textContent = 'Sending ' + method + ' ' + url + ' ...';
              try {{
                const res = await fetch(url, {{ method: method, cache: 'no-store' }});
                const ct = res.headers.get('content-type') || '';
                let text = '';
                if (ct.includes('application/json')) {{
                  text = JSON.stringify(await res.json(), null, 2);
                }} else if (ct.includes('image/') || ct.includes('audio/') || ct.includes('octet-stream')) {{
                  const blob = await res.blob();
                  text = 'HTTP ' + res.status + ' ' + res.statusText + '\nContent-Type: ' + ct + '\nBytes received: ' + blob.size;
                }} else {{
                  text = await res.text();
                }}
                out.textContent = 'HTTP ' + res.status + ' ' + res.statusText + '\n' + text;
              }} catch (e) {{
                out.textContent = 'Request failed: ' + e;
              }}
            }}
            async function tryPostDisabled(id, url) {{
              const body = document.getElementById('body_' + id).value || '';
              const out = document.getElementById('response_' + id);
              out.textContent = 'Sending POST ' + url + ' ...';
              try {{
                const res = await fetch(url, {{ method: 'POST', body: body, cache: 'no-store' }});
                const ct = res.headers.get('content-type') || '';
                const text = ct.includes('application/json') ? JSON.stringify(await res.json(), null, 2) : await res.text();
                out.textContent = 'HTTP ' + res.status + ' ' + res.statusText + '\n' + text;
              }} catch (e) {{
                out.textContent = 'Request failed: ' + e;
              }}
            }}
            function tryWebSocket(id, url) {{
              const out = document.getElementById('response_' + id);
              const displayUrl = url.startsWith('/') ? ((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + url) : url;
              out.textContent = 'Opening WebSocket ' + displayUrl + ' ...';
              try {{
                const wsUrl = url.startsWith('/') ? ((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + url) : url;
                const ws = new WebSocket(wsUrl);
                ws.binaryType = 'arraybuffer';
                ws.onopen = function() {{ out.textContent = 'WebSocket opened. Real video testing requires ESP32-CAM JPEG bytes.'; ws.close(); }};
                ws.onerror = function() {{ out.textContent = 'WebSocket connection failed. Check server, host, and network.'; }};
                ws.onclose = function() {{ out.textContent += '\nWebSocket closed.'; }};
              }} catch (e) {{
                out.textContent = 'WebSocket failed: ' + e;
              }}
            }}
          </script>
        </body>
        </html>
        """
    )


@app.get("/")
@app.get("/preview")
async def preview():
    rows = []
    for path, method, description in URL_ROWS:
        full_url = url_to_display(path)
        is_http_get = method == "GET" and full_url.startswith("http://")
        url_html = f'<a href="{full_url}">{full_url}</a>' if is_http_get else full_url
        rows.append(
            f"<tr>"
            f"<td><code>{method}</code></td>"
            f"<td><code>{url_html}</code></td>"
            f"<td>{description}</td>"
            f"</tr>"
        )
    html = f"""
    <html>
    <head>
      <title>Vidloq Stream Server</title>
      <style>
        body {{ font-family: Arial, sans-serif; margin: 24px; line-height: 1.4; background: #0f1117; color: #e8eaf0; }}
        table {{ border-collapse: collapse; width: 100%; background: #151924; }}
        th, td {{ border: 1px solid #30384a; padding: 8px; vertical-align: top; }}
        th {{ background: #202638; color: #ffffff; text-align: left; }}
        code {{ white-space: nowrap; color: #9ee6ff; }}
        a {{ color: #7cc7ff; }}
        .note {{ padding: 10px; background: #1f2637; border: 1px solid #3d4c70; margin: 16px 0; }}
        .footer {{ margin-top: 32px; color: #aab2c5; font-size: 14px; }}
      </style>
    </head>
    <body>
      <h1>Vidloq Stream Server</h1>
      <p>Server host: <strong>{SERVER_HOST}</strong></p>
      <p>HTTP port: <strong>8000</strong> | Raw continuous audio TCP port: <strong>{AUDIO_TCP_PORT}</strong></p>
      <div class="note">
        Some URLs are ingest endpoints and will not open in a browser. They are still listed because the ESP32-CAM, Heltec board, VLC, or Android app need them.
      </div>
      <h2>Valid URLs</h2>
      <table>
        <tr><th>Method / Type</th><th>URL</th><th>Description</th></tr>
        {''.join(rows)}
      </table>
      <h2>Recommended test order</h2>
      <ol>
        <li>Open <code>http://{SERVER_HOST}:8000/health</code>.</li>
        <li>Open <code>http://{SERVER_HOST}:8000/debug/latest.jpg</code>.</li>
        <li>Open <code>http://{SERVER_HOST}:8000/debug/audio_continuous.wav</code>.</li>
        <li>Only after video and audio debug URLs work, test <code>http://{SERVER_HOST}:8000/stream.ts</code> in VLC.</li>
      </ol>
      <div class="footer">Designed by Harold Paulino</div>
    </body>
    </html>
    """
    return HTMLResponse(html)


@app.get("/debug/latest.jpg")
async def debug_latest_jpg():
    if latest_jpeg is None:
        return Response(status_code=404, content=b"No JPEG received yet")
    return Response(content=latest_jpeg, media_type="image/jpeg")


@app.get("/debug/mjpeg")
async def debug_mjpeg():
    async def gen():
        last_id = -1
        while True:
            if latest_jpeg is not None and video_frame_count != last_id:
                last_id = video_frame_count
                yield b"--frame\r\nContent-Type: image/jpeg\r\n\r\n" + latest_jpeg + b"\r\n"
            await asyncio.sleep(0.2)
    return StreamingResponse(gen(), media_type="multipart/x-mixed-replace; boundary=frame")


@app.get("/debug/audio_level")
async def debug_audio_level():
    return {
        "available": audio_ring_size > 0,
        "peak": audio_level_peak,
        "peak_percent": round((audio_level_peak / 32768) * 100, 2),
        "audio_frame_count": audio_frame_count,
        "audio_tcp_frame_count": audio_tcp_frame_count,
        "audio_http_rejected_count": audio_http_rejected_count,
        "audio_sample_count": audio_sample_count,
        "seconds_available": round(audio_ring_size / (SAMPLE_RATE * BYTES_PER_SAMPLE), 2),
        "gap_samples_inserted": audio_gap_samples,
    }


@app.get("/debug/audio.wav")
@app.get("/debug/audio_continuous.wav")
async def debug_audio_wav():
    pcm = current_audio_bytes()
    if not pcm:
        return Response(status_code=404, content=b"No audio received yet")
    return Response(content=wav_response_bytes(pcm), media_type="audio/wav")


@app.get("/debug/audio_raw.pcm")
async def debug_audio_raw_pcm():
    pcm = current_audio_bytes()
    if not pcm:
        return Response(status_code=404, content=b"No audio received yet")
    return Response(content=pcm, media_type="application/octet-stream")


@app.post("/admin/reset")
async def admin_reset():
    global latest_jpeg, latest_video_time, latest_audio_time, video_frame_count, audio_frame_count, audio_tcp_frame_count
    global audio_sample_count, audio_bytes_total, audio_gap_samples, audio_bad_frames, audio_http_rejected_count
    global last_audio_seq, audio_ring, audio_ring_size, audio_level_peak, last_error
    async with state_lock:
        latest_jpeg = None
        latest_video_time = None
        latest_audio_time = None
        video_frame_count = 0
        audio_frame_count = 0
        audio_tcp_frame_count = 0
        audio_sample_count = 0
        audio_bytes_total = 0
        audio_gap_samples = 0
        audio_bad_frames = 0
        audio_http_rejected_count = 0
        last_audio_seq = None
        audio_ring = deque()
        audio_ring_size = 0
        audio_level_peak = 0
        last_error = None
    return {"ok": True}


@app.get("/stream.ts", response_model=None)
async def stream_ts():
    if latest_jpeg is None or audio_ring_size == 0:
        return JSONResponse(
            {"ok": False, "reason": "Need both video and continuous TCP audio before stream.ts"},
            status_code=503,
        )
    if shutil.which("ffmpeg") is None:
        return JSONResponse({"ok": False, "reason": "ffmpeg was not found in PATH"}, status_code=503)

    async def gen():
        q = asyncio.Queue(maxsize=300)
        stream_audio_queues.append(q)
        tmp = tempfile.TemporaryDirectory(prefix="esp32_stream_")
        audio_fifo = os.path.join(tmp.name, "audio.pcm")
        video_fifo = os.path.join(tmp.name, "video.mjpeg")
        os.mkfifo(audio_fifo)
        os.mkfifo(video_fifo)

        proc = None
        video_file = None
        audio_file = None
        video_task = None
        audio_task = None
        stderr_task = None
        last_video_sent = 0.0

        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-f", "mjpeg", "-r", "0.5", "-i", video_fifo,
            "-f", "s16le", "-ar", str(SAMPLE_RATE), "-ac", "1", "-i", audio_fifo,
            "-c:v", "mpeg1video", "-b:v", "350k", "-r", "0.5",
            "-c:a", "mp2", "-b:a", "64k",
            "-muxdelay", "0.001", "-f", "mpegts", "pipe:1",
        ]

        async def drain_stderr(p):
            try:
                while p.stderr and not p.stderr.at_eof():
                    await p.stderr.read(1024)
            except Exception:
                pass

        async def open_fifo_writer(path):
            return await asyncio.to_thread(open, path, "wb", buffering=0)

        async def feed_video(vf):
            nonlocal last_video_sent
            while proc and proc.returncode is None:
                frame = latest_jpeg
                if frame:
                    try:
                        await asyncio.to_thread(vf.write, frame)
                        last_video_sent = now()
                    except Exception:
                        break
                await asyncio.sleep(2.0)

        async def feed_audio(af):
            while proc and proc.returncode is None:
                try:
                    pcm = await asyncio.wait_for(q.get(), timeout=0.25)
                except asyncio.TimeoutError:
                    pcm = b"\x00\x00" * 800
                try:
                    await asyncio.to_thread(af.write, pcm)
                except Exception:
                    break

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stderr_task = asyncio.create_task(drain_stderr(proc))

            video_file, audio_file = await asyncio.wait_for(
                asyncio.gather(open_fifo_writer(video_fifo), open_fifo_writer(audio_fifo)),
                timeout=5.0,
            )

            video_task = asyncio.create_task(feed_video(video_file))
            audio_task = asyncio.create_task(feed_audio(audio_file))

            while proc.stdout:
                try:
                    chunk = await asyncio.wait_for(proc.stdout.read(4096), timeout=5.0)
                except asyncio.TimeoutError:
                    continue
                if not chunk:
                    break
                yield chunk
        except asyncio.CancelledError:
            raise
        except Exception as e:
            global last_error
            last_error = f"stream.ts error: {e}"
            return
        finally:
            try:
                stream_audio_queues.remove(q)
            except ValueError:
                pass
            for task in (video_task, audio_task, stderr_task):
                if task:
                    task.cancel()
            for f in (video_file, audio_file):
                if f:
                    try:
                        await asyncio.to_thread(f.close)
                    except Exception:
                        pass
            if proc and proc.returncode is None:
                try:
                    proc.terminate()
                    await asyncio.wait_for(proc.wait(), timeout=2.0)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
            try:
                tmp.cleanup()
            except Exception:
                pass

    return StreamingResponse(
        gen(),
        media_type="video/MP2T",
        headers={
            "Cache-Control": "no-store",
            "X-Accel-Buffering": "no",
        },
    )
