# This is a multi-stage build file
# First image is full of various compilation tools we need for
# binary modules. This image prepares $PYTHONUSERBASE folder contents
# This image is a throw-away. Only compiled artifacts
# (in a form of of $PYTHONUSERBASE folder contents)
# are extracted from this image and placed in second, pristine image
# with the goal of saving space in the final image.

###############
# Build Image #
###############
ARG BASE_IMAGE
FROM ${BASE_IMAGE} as pythonuserbaseimage

# In order to reuse Docker cache during builds
# requirements are split into base (those unlikely to change frequently)
# and regular requirements. The split is arbitrary and beside the
# desire to reuse Docker cache, there is nothing magic about the split.

# `--user` is used everywhere to ensure that modules are installed into
# known, extractable location. (Which is extracted further below)

COPY requirements.base.txt .
RUN pip install --user --no-cache-dir -r requirements.base.txt
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt
# This one needs to move out of the base image as we approach prod time
# to avoid piling up useless code into prod images.
# TODO: discuss how build AND USE of dev image will look like.
COPY requirements.dev.txt .
RUN pip install --user --no-cache-dir -r requirements.dev.txt

##############
# Prod Image #
##############

ARG BASE_IMAGE
FROM ${BASE_IMAGE}

# to allow Celery work as root
ENV C_FORCE_ROOT=true

COPY --from=pythonuserbaseimage $PYTHONUSERBASE/ $PYTHONUSERBASE/

COPY . .

ARG BUILD_ID=latest
ARG COMMIT_ID=none
# these env vars are picked up by config's update_from_env function
# and applied to config.build.BUILD_ID config.build.COMMIT_ID attrs.
ENV APP_BUILD_BUILD_ID=${BUILD_ID}
ENV APP_BUILD_COMMIT_ID=${COMMIT_ID}
