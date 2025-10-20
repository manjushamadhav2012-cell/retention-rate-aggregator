# Python Project: WH Assessment

This project processes and analyzes student data from a public API, saving results in CSV and Parquet formats.  
It includes a data pipeline, tests, and is fully containerized for development and testing.

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/)
or
- [Docker Desktop](https://docs.docker.com/desktop/) which includes docker compose

### Setup

1. **Clone the repository:**
   ```bash
   git clone <your-repo-url>
   cd Python_Project
   ```

2. **(Optional) Review and edit `requirements.txt`**  
   Ensure all required Python packages are listed.

### Build and Start a Development Container

```bash
docker-compose run assessment-service-dev
```
if using Docker Desktop

```bash
docker compose run assessment-service-dev
```


This will open a shell inside the container with your code mounted at `/app`.

### Run the Main Script

Inside the container shell:
```bash
python retention_rate_second_level_school.py
```

### Run Tests

Inside the container shell:
```bash
pytest tests
pytest --cov=. --cov-report=term-missing tests
```

### Project Structure

```
Python_Project/
├── retention_rate_second_level_school.py
├── utils.py
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── transformed/           # Output files (created automatically)
└── tests/
    └── test_retention_rate_second_level_school.py
```

### Notes

- All output files are saved in the `transformed` directory.
- Code changes on your host are reflected in the container (thanks to the volume mount).
- You can install additional packages by adding them to `requirements.txt` and rebuilding the container.