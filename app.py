"""
Hugging Face Spaces entry point.
Runs a simple health check HTTP server alongside the Discord bot.
"""

import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler


class HealthHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler for health checks."""

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b'''
        <!DOCTYPE html>
        <html>
        <head><title>CP2077 Bot Status</title></head>
        <body style="font-family: Arial; padding: 20px; background: #1a1a2e; color: #eee;">
            <h1>🤖 CP2077 Discord Bot</h1>
            <p style="color: #4ade80;">✅ Bot is running!</p>
            <p>This is a Discord bot for competitive programming.</p>
        </body>
        </html>
        ''')

    def log_message(self, format, *args):
        pass  # Suppress HTTP request logging


def run_health_server():
    """Start HTTP server on port 7860 for HF Spaces health checks."""
    port = int(os.environ.get('PORT', 7860))
    server = HTTPServer(('0.0.0.0', port), HealthHandler)
    print(f'Health check server running on port {port}')
    server.serve_forever()


if __name__ == '__main__':
    # Start health check server in background thread
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()

    # Run the Discord bot (this blocks)
    from tle.__main__ import main
    main()
