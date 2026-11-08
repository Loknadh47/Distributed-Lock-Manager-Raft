FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/app
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto ./proto
COPY dlm ./dlm
RUN python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/dlm/v1/dlm.proto

RUN useradd --system --uid 10001 dlm && mkdir /data && chown dlm /data
USER dlm
ENV DATA_DIR=/data
VOLUME /data
EXPOSE 50051

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
  CMD python -c "import socket; socket.create_connection(('127.0.0.1', 50051), 2)"

ENTRYPOINT ["python", "-m"]
CMD ["dlm.server"]
