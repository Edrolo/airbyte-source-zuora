# Build a standalone connector image for self-hosted Airbyte (Docker/Kubernetes).
#
# The connector normally declares a base-image build in metadata.yaml (for Airbyte's
# `airbyte-ci` tooling); this Dockerfile is the standalone equivalent so the image can
# be built and pushed to any registry without the Airbyte monorepo.
#
# Build (for a typical amd64 cluster, e.g. from an Apple-Silicon dev machine):
#   docker build --platform linux/amd64 -t <registry>/source-zuora:0.2.0 .
#
FROM docker.io/airbyte/python-connector-base:2.0.0

# Install the connector package (registers the `source-zuora` console script via
# pyproject.toml [tool.poetry.scripts]). pip resolves deps from pyproject at build time.
COPY . ./airbyte/integration_code
RUN pip install --no-cache-dir ./airbyte/integration_code

# Airbyte invokes the image with the protocol command as args: spec / check / discover / read.
ENV AIRBYTE_ENTRYPOINT="source-zuora"
ENTRYPOINT ["source-zuora"]
