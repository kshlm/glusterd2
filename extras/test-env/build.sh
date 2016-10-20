#!/usr/bin/env bash
set -e
set -x

# This script builds the glusterd2-test docker image.
# It is mainly designed to run on the Docker Hub via the hooks mechanism just
# so we can pass the build arg GD2_VERSION onto setup.sh.
# It can run locally as well.

# When run in Docker Hub DOCKER_TAG and IMAGE_NAME are set based on GD2 version
# for which the image is being built.
# Hub needs to have build with trigger settings as follows,
#  Type: Tag
#  Name: /^v4\.0dev\-[0-9.]+$/
#  Docker tag name: {sourceref}
# This should trigger builds for new tags.

# If not running in the hub environment set up the hub variables to use
if [ "x${IMAGE_NAME}" = "x" -o "x${DOCKER_TAG}" = "x" ]; then
  pkg_version="$(dirname $0)/../../scripts/pkg-version"
  # Doing it this way because pkg-version returns a dirty version when there
  # are commits beyond the latest tag
  GD2_VERSION="v$($pkg_version --version)-$($pkg_version --release | cut -d. -f1)"

  IMAGE_NAME="gluster/glusterd2-test:${GD2_VERSION}"
else
  GD2_VERSION=${DOCKER_TAG}
fi

docker build --build-arg=GD2_VERSION=$GD2_VERSION -t $IMAGE_NAME .


