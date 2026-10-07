# Base Python Image
FROM python:3.10-slim

# Workspace
WORKDIR /app

# 1. Installation of git
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# 2. Copying files
COPY requirements.txt .

# 3. Install requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy code
COPY . .

# 5. Start Docker Machine
CMD ["python", "producer.py"]
