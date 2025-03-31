# Use a lightweight Python image
FROM python:3.9

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# Run the Python script
CMD ["python", "main.py"]
