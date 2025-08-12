# Use the official Python 3.13.5 image based on Debian Bookworm
FROM python:3.13.5-slim-bookworm

# Accept host locale or fallback to en_US.UTF-8
ARG BUILD_LANG=en_US.UTF-8

# Install system dependencies & configure locale
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        locales \
    && rm -rf /var/lib/apt/lists/* \
    && if [ -z "$BUILD_LANG" ]; then \
           echo "No BUILD_LANG provided — using C.UTF-8"; \
           BUILD_LANG="C.UTF-8"; \
       fi \
    && if grep -q "$BUILD_LANG" /etc/locale.gen; then \
           sed -i "/$BUILD_LANG/s/^# //g" /etc/locale.gen && locale-gen "$BUILD_LANG"; \
       else \
           echo "Warning: $BUILD_LANG not found — using C.UTF-8"; \
           sed -i '/C.UTF-8/s/^# //g' /etc/locale.gen && locale-gen C.UTF-8 \
           && BUILD_LANG="C.UTF-8"; \
       fi

# Set environment variables for all processes
ENV LANG=$BUILD_LANG \
    LC_ALL=$BUILD_LANG \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Create a custom user with UID 1234 and GID 1234
RUN groupadd -g 1234 pygroup && \
    useradd -m -u 1234 -g pygroup pyuser


# Copy dependency list
COPY requirements.txt .

# Upgrade pip and install dependencies
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt


# Switch to the custom user
USER pyuser

# Set the workdir
WORKDIR /home/pyuser

ENV PATH="/home/pyuser/.local/bin:$PATH"

# Copy the rest of your app code (optional here)
# COPY . .

# Set default command
CMD ["python"]
