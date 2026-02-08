FROM python:3.11-slim-bookworm

# Install system dependencies
# Debian 12 (Bookworm) has Python 3.11 as system python
# We install python3 and system cairo libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    libcairo2 gir1.2-pango-1.0 \
    gobject-introspection python3-gi python3-gi-cairo python3-cairo \
    libjpeg-dev zlib1g-dev \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

ENV FONTCONFIG_FILE=/bot/extra/fonts.conf
ENV PYTHONUNBUFFERED=1

WORKDIR /bot
COPY pyproject.toml .

# Install dependencies using system pip
# Since system python is 3.11, this matches what we need
RUN /usr/bin/python3 -m pip install --break-system-packages --no-cache-dir .

COPY . .

# Run with system python3
CMD ["/usr/bin/python3", "-m", "tle"]
