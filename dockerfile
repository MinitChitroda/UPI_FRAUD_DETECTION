# Use a slim Python image to reduce container size
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy your code and files
COPY pipeline/ ./pipeline/
COPY model/ ./model/
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Ensure logs are shown immediately (not buffered)
ENV PYTHONUNBUFFERED=1

# Run the fraud detection service
CMD ["python", "pipeline/data_fetcher.py"]
