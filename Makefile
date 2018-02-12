default:
	@echo "select distinct make action"

clean:
	find . -name \*~ -type f -delete
	find . -name \*.pyc -type f -delete

.PHONY: default clean

VENDOR_NAME:=metrics
IMAGE_NAME:=collection-system
IMAGE_NAME_FULL:=$(VENDOR_NAME)/$(IMAGE_NAME)

# When we are building through Circle CI, use the
BRANCH_NAME:=$(if $(CIRCLE_BRANCH),$(CIRCLE_BRANCH),$(shell git rev-parse --abbrev-ref HEAD))
BUILD_ID?=latest
BUILD_ID:=$(if $(CIRCLE_BUILD_NUM),$(CIRCLE_BUILD_NUM),$(BUILD_ID))
COMMIT_ID=$(if $(CIRCLE_SHA1),$(CIRCLE_SHA1),$(shell git log -1 --format="%H"))

PYTHONUSERBASE_INNER=/tmp/pythonuserbase
WORKDIR:=/usr/src/app

image.base:
	docker build \
		-t $(IMAGE_NAME_FULL)-base:$(BUILD_ID) \
		--build-arg PYTHONUSERBASE=$(PYTHONUSERBASE_INNER) \
		--build-arg WORKDIR=$(WORKDIR) \
		-f docker/Dockerfile.base .

image: image.base
	docker build \
		-t $(IMAGE_NAME_FULL):$(BUILD_ID) \
		--build-arg BASE_IMAGE=$(IMAGE_NAME_FULL)-base:$(BUILD_ID) \
		--build-arg COMMIT_ID=${COMMIT_ID} \
		--build-arg BUILD_ID=${BUILD_ID} \
		--build-arg PYTHONUSERBASE=$(PYTHONUSERBASE_INNER) \
		-f docker/Dockerfile .

.PHONY: image image.base

#############
# Dynamodb local management
DYNAMO_IMAGE_NAME:="dynamodb"
DYNAMO_IMAGE_NAME_FULL:=$(VENDOR_NAME)/$(DYNAMO_IMAGE_NAME)

# Build DynamoDB image
image.dynamo:
	docker build \
		--no-cache \
		-t $(DYNAMO_IMAGE_NAME_FULL) \
		--build-arg DYNAMODB_VERSION=latest \
		-f docker/Dockerfile.dynamodb .

.PHONY: image.dynamo

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

# use this for interactive console dev and running unit tests
start-dev:
	DYNAMO_IMAGE_NAME_FULL=$(DYNAMO_IMAGE_NAME_FULL) \
	DYNAMODIR=/dynamodb_local_db \
	IMAGE_NAME_FULL=$(IMAGE_NAME_FULL) \
	USER_ID=$(shell id -u) \
	GROUP_ID=$(shell id -g) \
	WORKDIR=$(WORKDIR) \
	docker-compose -f docker/docker-compose-dev.yaml run --service-ports app

# use this for standing up entire stack on its own and interacting with it remotely
start-stack:
	DYNAMO_IMAGE_NAME_FULL=$(DYNAMO_IMAGE_NAME_FULL) \
	DYNAMODIR=/dynamodb_local_db \
	IMAGE_NAME_FULL=$(IMAGE_NAME_FULL) \
	USER_ID=$(shell id -u) \
	GROUP_ID=$(shell id -g) \
	WORKDIR=$(WORKDIR) \
	docker-compose -f docker/docker-compose-stack.yaml up

.PHONY: start-dev start-stack


#############
# requirement files management
requirements-compile:
	pip-compile requirements.base.src && \
	pip-compile requirements.src && \
	pip-compile requirements.dev.src
