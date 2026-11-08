.PHONY: proto test lint demo up down logs clean

proto:  ## generate gRPC stubs into dlm/v1/
	python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/dlm/v1/dlm.proto

test:  ## run the test suite (gRPC e2e test is skipped until `make proto` has run)
	python -m pytest

lint:
	ruff check .

demo:  ## in-process 3-node cluster, no Docker/gRPC needed
	PYTHONPATH=. python examples/local_demo.py

up:  ## 3-node cluster in Docker
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f

clean:
	rm -rf .pytest_cache .ruff_cache dlm/v1/*_pb2*.py data
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
