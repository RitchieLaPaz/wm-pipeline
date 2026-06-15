# Official Playwright Python image — Chromium + all system deps pre-installed
# This is the correct base for Railway. No extra apt-get installs needed.
FROM mcr.microsoft.com/playwright/python:v1.48.0-noble

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir

COPY . .

# Railway runs this as the cron job command
CMD ["python", "scraper.py"]
