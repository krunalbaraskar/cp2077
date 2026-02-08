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
        self.wfile.write('''
        <!DOCTYPE html>
        <html>
        <head><title>CP2077 Bot Status</title></head>
        <body style="font-family: Arial; padding: 20px; background: #1a1a2e; color: #eee;">
            <h1>🤖 CP2077 Discord Bot</h1>
            <p style="color: #4ade80;">✅ Bot is running!</p>
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


if __name__ == '__main__':
    import time
    import socket
    
    # Start health check server in background thread
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()

    # Configure DNS at runtime (backup for container restarts)
    try:
        with open('/etc/resolv.conf', 'w') as f:
            f.write("nameserver 8.8.8.8\n")
            f.write("nameserver 1.1.1.1\n")
            f.write("nameserver 8.8.4.4\n")
        print("DNS configured successfully")
    except PermissionError:
        print("Could not write DNS config (running as non-root)")
    except Exception as e:
        print(f"DNS config warning: {e}")

    # Test DNS resolution before starting bot
    def test_dns(host="discord.com", retries=5):
        for attempt in range(retries):
            try:
                result = socket.gethostbyname(host)
                print(f"DNS resolution successful: {host} -> {result}")
                return True
            except socket.gaierror as e:
                wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4, 8, 16 seconds
                print(f"DNS resolution attempt {attempt + 1}/{retries} failed: {e}")
                if attempt < retries - 1:
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
        return False

    # Run the Discord bot with retry logic
    max_retries = 5
    for attempt in range(max_retries):
        try:
            if not test_dns():
                print("DNS resolution failed after retries, but attempting bot start anyway...")
            
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
