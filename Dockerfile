FROM python:3.7

MAINTAINER support@cognite.com

ENV PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100 \
  POETRY_VERSION=0.12.16

# System deps:
RUN pip install "poetry==$POETRY_VERSION"

WORKDIR /code

# Creating folders, and files for a project:
COPY . /code

# Project initialization:
RUN poetry config settings.virtualenvs.create false && poetry install --no-dev --no-interaction --no-ansi

ENTRYPOINT ["python", "-m", "cognite.replicator"]
CMD []
