"""
Hugging Face Spaces entry point.
Runs a simple health check HTTP server alongside the Discord bot.

DNS FIX: aiodns (c-ares) reads its own resolv.conf which HF Spaces
overrides at runtime. We force aiohttp to use ThreadedResolver instead,
which delegates to stdlib socket.getaddrinfo (the same path urllib uses,
and that works fine for downloading fonts etc.).
"""

import os
import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

# ── DNS Fix: force aiohttp to use ThreadedResolver ─────────────────────
# This MUST happen before discord.py or aiohttp is imported, because
# aiohttp checks for aiodns at import time and auto-selects AsyncResolver.
# By removing aiodns from the importable modules, aiohttp falls back to
# ThreadedResolver which uses the working stdlib socket.getaddrinfo.
#
# This is safe — we keep aiodns installed (supabase/other deps may want it)
# but prevent aiohttp's auto-detection from picking it up.
import importlib

# Block aiohttp from detecting aiodns
sys.modules['aiodns'] = None  # type: ignore[assignment]
print("DNS fix applied: forced aiohttp to use ThreadedResolver (stdlib socket)")


class HealthHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler for health checks."""

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write('''
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>CP2077 Bot Status</title>
        </head>
        <body style="font-family: Arial; padding: 20px; background: #1a1a2e; color: #eee;">
            <h1>CP2077 Discord Bot</h1>
            <p style="color: #4ade80;">Bot is running!</p>
            <p>This is a Discord bot for competitive programming.</p>
        </body>
        </html>
        '''.encode('utf-8'))

    def log_message(self, format, *args):
        pass  # Suppress HTTP request logging


def run_health_server():
    """Start HTTP server on port 7860 for HF Spaces health checks."""
    port = int(os.environ.get('PORT', 7860))
    server = HTTPServer(('0.0.0.0', port), HealthHandler)
    print(f'Health check server running on port {port}')
    server.serve_forever()


def try_fix_resolv_conf():
    """Try to write working DNS servers to resolv.conf (best-effort)."""
    resolv_content = "nameserver 8.8.8.8\nnameserver 1.1.1.1\nnameserver 8.8.4.4\n"
    try:
        with open('/etc/resolv.conf', 'r') as f:
            current = f.read().strip()
        print(f"Current resolv.conf:\n{current}")

        # Only rewrite if it looks broken (no nameserver lines)
        if 'nameserver' not in current:
            try:
                with open('/etc/resolv.conf', 'w') as f:
                    f.write(resolv_content)
                print("Rewrote /etc/resolv.conf with Google/Cloudflare DNS")
            except PermissionError:
                print("Cannot write /etc/resolv.conf (permission denied, non-root)")
    except Exception as e:
        print(f"Could not read resolv.conf: {e}")


if __name__ == '__main__':
    import time
    import socket

    print(f"===== Application Startup at {time.strftime('%Y-%m-%d %H:%M:%S')} =====")

    # Start health check server in background thread
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()

    # Print network diagnostics
    print("=== Network Diagnostics ===")
    try_fix_resolv_conf()

    # Check for proxy env vars
    for var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'NO_PROXY']:
        val = os.environ.get(var)
        if val:
            print(f"  {var}={val}")
    print("===========================")

    # Quick DNS sanity check (non-blocking, just informational)
    def test_dns(host="discord.com"):
        try:
            result = socket.gethostbyname(host)
            print(f"DNS check: {host} -> {result}")
            return True
        except socket.gaierror as e:
            print(f"DNS check: {host} failed ({e}), but ThreadedResolver should handle it")
            return False

    test_dns()

    # Run the Discord bot with retry logic
    import asyncio
    max_retries = 5
    for attempt in range(max_retries):
        try:
            # Create a fresh event loop for each attempt.
            # bot.run() closes the event loop when it exits (even on failure),
            # so subsequent retries would fail with "Event loop is closed".
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            from tle.__main__ import main
            print(f"Starting Discord bot (attempt {attempt + 1}/{max_retries})...")
            main()
            break  # If main() exits normally, break
        except Exception as e:
            wait_time = 2 ** attempt
            print(f"Bot startup failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print("Max retries reached. Exiting.")
                raise
