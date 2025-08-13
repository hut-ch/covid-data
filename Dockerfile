# Stage 1: Build dependencies and create wheels (optional, for faster installs)
FROM python:3.13.5-slim-bookworm AS builder

# Install system dependencies needed for building packages
RUN apt-get update && apt-get install -y build-essential \
    && rm -rf /var/lib/apt/lists/* 

# Set environment variables for this stage
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Copy dependency list
COPY requirements.txt .

# Upgrade pip and install dependencies, creating wheels for the next stage
RUN pip install --upgrade pip \
    && pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt \
    && rm -rf /root/.cache/pip

# Stage 2: Production image
FROM python:3.13.5-slim-bookworm

#  Set locale variables for the final image (if needed, otherwise remove)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Create a custom user with UID 1234 and GID 1234
RUN groupadd -g 1234 pygroup && \
    useradd -m -u 1234 -g pygroup pyuser

# Switch to the custom user
USER pyuser

# Set the workdir
WORKDIR /home/pyuser

ENV PATH="/home/pyuser/.local/bin:$PATH"

# Copy the built wheels from the builder stage
COPY --from=builder /wheels /wheels

# Copy requirements.txt again for installation within this stage
COPY requirements.txt .

# Install dependencies from the wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels -r requirements.txt \
    && rm -rf /wheels

# Copy the app code
# COPY . .

# Set default command
CMD ["python"]
