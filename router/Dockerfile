FROM python:3.11-slim
RUN pip install flask requests --no-cache-dir
WORKDIR /app
COPY router.py .
CMD ["python", "router.py"]
