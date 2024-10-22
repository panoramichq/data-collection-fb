default:
	@echo "select distinct make action"

clean:
	find . -name \*~ -type f -delete
	find . -name \*.pyc -type f -delete
	rm -rf .dynamodb_data
	rm -rf .s3_data
	rm -rf .pytest_cache

.PHONY: default clean

# Operam Python base image from Docker Hub
BASE_IMAGE_NAME:=operam/base-images:python3.6-latest

APP_SLUG:=data-collection-fb

VENDOR_NAME:=operam
IMAGE_NAME?=$(APP_SLUG)
IMAGE_NAME_FULL?=$(VENDOR_NAME)/$(IMAGE_NAME)

# When we are building through Circle CI, use the
BRANCH_NAME?=$(shell git rev-parse --abbrev-ref HEAD)
COMMIT_ID?=$(shell git rev-parse --short HEAD)
BUILD_ID?=latest

# Datadog instrumentation agent
DDOG_IMAGE_NAME:=datadog/docker-dd-agent
DDOG_IMAGE_VERSION:=12.5.5223-dogstatsd-alpine
DDOG_IMAGE_NAME_FULL:=$(DDOG_IMAGE_NAME):$(DDOG_IMAGE_VERSION)
DDOG_HOSTNAME:=$(VENDOR_NAME)-$(IMAGE_NAME)-datadog
# Setting this to weird value won't allow the agent to actually submit stuff
DDOG_API_KEY:=$(if $(DDOG_API_KEY),$(DDOG_API_KEY),__none__)

PYTHONUSERBASE_INNER=/usr/src/lib
WORKDIR=/usr/src/app

# intentionally empty. Simulates url-less treatment of Docker Hub push.
# Inject something like:
# 936368275341.dkr.ecr.us-east-1.amazonaws.com/
# !!!!!!! Note the trailing slash !!!!!!!!!!! ^
REPOSITORY_URL?=

PUSH_IMAGE_NAME_PREFIX?=$(REPOSITORY_URL)$(IMAGE_NAME_FULL)
PUSH_IMAGE_NAME_BRANCH?=$(PUSH_IMAGE_NAME_PREFIX):$(BRANCH_NAME)
# Docker image tags size limit is 128 characters.
# Normal Commit ID is 40 chars. We use short commit IDs - 7 chars
# Should be enough.
PUSH_IMAGE_TAG?=$(BUILD_ID)-$(BRANCH_NAME)-$(COMMIT_ID)
PUSH_IMAGE_NAME_BUILD?=$(PUSH_IMAGE_NAME_PREFIX):$(PUSH_IMAGE_TAG)

image:
	docker build \
		-t $(IMAGE_NAME_FULL):$(BUILD_ID) \
		--build-arg BASE_IMAGE=$(BASE_IMAGE_NAME) \
		--build-arg COMMIT_ID=${COMMIT_ID} \
		--build-arg BUILD_ID=${BUILD_ID} \
		--build-arg PYTHONUSERBASE=$(PYTHONUSERBASE_INNER) \
		-f Dockerfile .

push_image:
	docker tag $(IMAGE_NAME_FULL):$(BUILD_ID) $(PUSH_IMAGE_NAME_BUILD)
	docker push $(PUSH_IMAGE_NAME_BUILD)

# used in Circle CI to save image ID between build steps
print_image_name_build:
	@printf "$(PUSH_IMAGE_NAME_BUILD)"

.PHONY: image push_image print_image_name_build

#############
# Dev Helpers

# Copying PYTHONUSEBASE folder from container to local machine to allow IDE introspection
# without running PIP install on the host.
CONTAINER_NAME:=$(VENDOR_NAME)-$(IMAGE_NAME)-container
rm-container:
	@docker rm -f $(CONTAINER_NAME) || true

PYTHONUSERBASE?=/tmp/$(IMAGE_NAME_FULL)/pythonuserbase
pythonuserbase: rm-container
	@mkdir -p $(PYTHONUSERBASE)
	@docker create --name $(CONTAINER_NAME) $(IMAGE_NAME_FULL):latest && \
		docker cp $(CONTAINER_NAME):$(PYTHONUSERBASE_INNER)/. $(PYTHONUSERBASE)/
	@echo "add to your IDE's python paths: ${PYTHONUSERBASE}/lib/python3.6/site-packages/"

.PHONY: pythonuserbase rm-container


DYNAMODIR:=/dynamodb_local_db
FAKES3DIR:=/s3_data

.dynamodb_data:
	mkdir -p .dynamodb_data

.s3_data:
	mkdir -p .s3_data

# use this for interactive console dev and running unit tests
start-dev: .dynamodb_data .s3_data
	DYNAMODIR=$(DYNAMODIR) \
	FAKES3DIR=$(FAKES3DIR) \
	DDOG_IMAGE_NAME_FULL=${DDOG_IMAGE_NAME_FULL} \
	DDOG_HOSTNAME=${DDOG_HOSTNAME} \
	DDOG_API_KEY=${DDOG_API_KEY} \
	IMAGE_NAME_FULL=$(IMAGE_NAME_FULL) \
	USER_ID=$(shell id -u) \
	GROUP_ID=$(shell id -g) \
	WORKDIR=$(WORKDIR) \
	docker-compose -f docker-compose-dev.yaml run --service-ports app

# use this for standing up entire stack on its own and interacting with it remotely
start-stack: .dynamodb_data .s3_data
	DYNAMODIR=$(DYNAMODIR) \
	FAKES3DIR=$(FAKES3DIR) \
	DDOG_IMAGE_NAME_FULL=${DDOG_IMAGE_NAME_FULL} \
	DDOG_HOSTNAME=${DDOG_HOSTNAME} \
	DDOG_API_KEY=${DDOG_API_KEY} \
	IMAGE_NAME_FULL=$(IMAGE_NAME_FULL) \
	USER_ID=$(shell id -u) \
	GROUP_ID=$(shell id -g) \
	WORKDIR=$(WORKDIR) \
	docker-compose -f docker-compose-stack.yaml up

# use this to completely remove the stack containers
drop-stack:
	DYNAMODIR=$(DYNAMODIR) \
	FAKES3DIR=$(FAKES3DIR) \
	DDOG_IMAGE_NAME_FULL=${DDOG_IMAGE_NAME_FULL} \
	DDOG_HOSTNAME=${DDOG_HOSTNAME} \
	DDOG_API_KEY=${DDOG_API_KEY} \
	IMAGE_NAME_FULL=$(IMAGE_NAME_FULL) \
	USER_ID=$(shell id -u) \
	GROUP_ID=$(shell id -g) \
	WORKDIR=$(WORKDIR) \
	docker-compose -f docker-compose-stack.yaml down

# Use this for standing up services in docker, but not the app (that can be ran on host). Useful for local dev.
start-services: .dynamodb_data .s3_data
	DYNAMODIR=$(DYNAMODIR) \
	FAKES3DIR=$(FAKES3DIR) \
	DDOG_IMAGE_NAME_FULL=${DDOG_IMAGE_NAME_FULL} \
	DDOG_HOSTNAME=${DDOG_HOSTNAME} \
	DDOG_API_KEY=${DDOG_API_KEY} \
	IMAGE_NAME_FULL=$(IMAGE_NAME_FULL) \
	USER_ID=$(shell id -u) \
	GROUP_ID=$(shell id -g) \
	WORKDIR=$(WORKDIR) \
	docker-compose -f docker-compose-services.yaml up

.PHONY: start-dev start-stack drop-stack

#############
# Test runner
test: .dynamodb_data .s3_data
	DYNAMODIR=$(DYNAMODIR) \
	FAKES3DIR=$(FAKES3DIR) \
	IMAGE_NAME_FULL=$(IMAGE_NAME_FULL):$(BUILD_ID) \
	WORKDIR=/usr/src/app \
	docker-compose -f docker-compose-test.yaml run \
		--rm app

.PHONY: test

#############
# requirement files management
.PHONY: requirements-compile

requirements-compile:
	docker run \
		-v ${HOME}/.pip/pip.conf:/etc/pip.conf \
		-v $(PWD):$(WORKDIR) \
		--workdir $(WORKDIR) \
		--rm ${BASE_IMAGE_NAME} \
		/bin/bash -c "\
			pip install \
				--upgrade \
				pip-tools pip && \
			pip-compile --build-isolation --allow-unsafe --no-emit-index-url --generate-hashes requirements.base.src && \
			pip-compile --build-isolation --allow-unsafe --no-emit-index-url --generate-hashes requirements.src && \
			pip-compile --build-isolation --allow-unsafe --no-emit-index-url --generate-hashes requirements.dev.src"


flake8:
	docker run \
		-v $(PWD):$(WORKDIR) \
		--rm $(IMAGE_NAME_FULL):$(BUILD_ID) \
		/bin/bash -c "flake8 --filename=*.py"

.PHONY: flake8

black:
	docker run \
		-v $(PWD):$(WORKDIR) \
		--rm $(IMAGE_NAME_FULL):$(BUILD_ID) \
		/bin/bash -c "black ."

.PHONY: black

black-check:
	docker run \
		-v $(PWD):$(WORKDIR) \
		--rm $(IMAGE_NAME_FULL):$(BUILD_ID) \
		/bin/bash -c "black --diff --check ."

.PHONY: black-check
