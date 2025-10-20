FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy all code (for initial build; live code will be mounted as a volume)
COPY . .

# Default: start a shell for development
CMD ["bash"]