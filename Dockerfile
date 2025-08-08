# Use the official Python 3.13.5 image based on Debian Bookworm
FROM python:3.13.5-slim-bookworm

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Install build-essential for compiling C++ code
# This needs to be done *before* installing any packages that might require compilation
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*


# Create a custom user with UID 1234 and GID 1234
RUN groupadd -g 1234 pygroup && \
    useradd -m -u 1234 -g pygroup pyuser


# Copy dependency list
COPY requirements.txt .

# Upgrade pip and install dependencies
RUN pip install --upgrade pip && pip install -r requirements.txt


# Switch to the custom user
USER pyuser

# Set the workdir
WORKDIR /home/pyuser

ENV PATH="/home/pyuser/.local/bin:$PATH"

# Copy the rest of your app code (optional here)
# COPY . .

# Set default command
CMD ["python"]
