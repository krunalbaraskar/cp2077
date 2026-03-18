FROM python:3.9-slim-bookworm

# Install system dependencies
# Debian 12 (Bookworm) has Python 3.11 as system python
# We install python3 and system cairo libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    libcairo2 gir1.2-pango-1.0 \
    gobject-introspection python3-gi python3-gi-cairo python3-cairo \
    libjpeg-dev zlib1g-dev \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Configure DNS - COPY works here because it writes at the image layer level,
# unlike RUN which uses Docker's read-only mounted /etc/resolv.conf
COPY resolv.conf /etc/resolv.conf

# Create user that HF Spaces expects (UID 1000)
RUN useradd -m -u 1000 user

ENV FONTCONFIG_FILE=/bot/extra/fonts.conf
ENV PYTHONUNBUFFERED=1

WORKDIR /bot
COPY --chown=user:user pyproject.toml .

# Install dependencies using system pip
RUN /usr/bin/python3 -m pip install --break-system-packages --no-cache-dir .

COPY --chown=user:user . .

# Pre-create all data/log directories that the bot needs at runtime
# so the non-root user can write to them
RUN mkdir -p data/assets/fonts data/db data/misc data/temp logs \
    && chown -R user:user data logs

# Switch to the non-root user
USER user

# Expose port for HF Spaces health check
EXPOSE 7860

# Run with app.py (includes health check server)
CMD ["/usr/bin/python3", "app.py"]
