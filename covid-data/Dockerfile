# Use the official Python 3.13.5 image based on Debian Bookworm
FROM python:3.13.5-bookworm

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Copy dependency list
COPY requirements.txt .

# Upgrade pip and install dependencies
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy the rest of your app code (optional here)
RUN mkdir /covid-data
#COPY /src /covid-data
WORKDIR /covid-data

# Set default command
CMD ["bash"]