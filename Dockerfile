# Use the official Python image from DockerHub
FROM jupyter/pyspark-notebook

# Copy the application code into the container
COPY . /app

# Set working directory
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Run the weather stream script
CMD ["python", "weather_stream.py"]
