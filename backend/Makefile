build:
	docker build -t imms-lambda-build -f lambda.Dockerfile .

package: build
	mkdir -p build
	docker run -d -p 8080:8080 --rm -v $(shell pwd)/build:/build imms-lambda-build

test:
	python -m unittest

.PHONY: build package test
