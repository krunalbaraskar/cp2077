FROM python:3.9-slim-bookworm

# Install system dependencies
# Debian 12 (Bookworm) has Python 3.11 as system python
# We install python3 and system cairo libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    libcairo2 gir1.2-pango-1.0 \
    gobject-introspection python3-gi python3-gi-cairo python3-cairo \
    libjpeg-dev zlib1g-dev \
    python3-pip \
    dnsutils iputils-ping \
    && rm -rf /var/lib/apt/lists/* \
    && echo "nameserver 8.8.8.8" > /etc/resolv.conf \
    && echo "nameserver 1.1.1.1" >> /etc/resolv.conf

ENV FONTCONFIG_FILE=/bot/extra/fonts.conf
ENV PYTHONUNBUFFERED=1

WORKDIR /bot
COPY pyproject.toml .

# Install dependencies using system pip
# Since system python is 3.11, this matches what we need
RUN /usr/bin/python3 -m pip install --break-system-packages --no-cache-dir .

COPY . .

# Expose port for HF Spaces health check
EXPOSE 7860

# Run with app.py (includes health check server)
CMD ["/usr/bin/python3", "app.py"]
