# Use a lightweight Python base image
FROM python:3.9-slim

# 1. Install Java (Required for PySpark)
# We update apt, install OpenJDK 17, and clean up to keep image small
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get clean;

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 2. Set up working directory
WORKDIR /app

# 3. Install Python dependencies
# We install PySpark and Delta Lake (optional but good for future)
RUN pip install pyspark delta-spark

# 4. Copy your local code into the container
COPY . /app

# 5. Default command: Run the pipeline test
CMD ["python", "lakehouse/test/run_pipeline_test.py"]
