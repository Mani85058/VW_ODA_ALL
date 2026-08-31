FROM sqa-docker.docker.devstack.vwgroup.com/pyspark-delta:0.91

# URL and credentials for JFrog Artifactory access
ARG JFROG_USER
ARG JFROG_PWD
ARG INDEXURL=https://${JFROG_USER}:${JFROG_PWD}@jfrog.devstack.vwgroup.com/artifactory/api/pypi/pypi/simple

ENV USERNAME=${JFROG_USER}
ENV PASSWORD=${JFROG_PWD}

# Do not cache Python packages
ENV PIP_NO_CACHE_DIR=yes

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1

# Suppress Pipenv progress indicators to avoid polluting the logs
ENV CI=1

# Set the locale default
ENV LANG C.UTF-8

# Set the working directory in the container
WORKDIR /opt/spark/work-dir

# Copy Pipfile, Pipfile.lock, and source code to the working directory
COPY src ./src
COPY config ./config
COPY tests ./tests
COPY Pipfile ./Pipfile
COPY Pipfile.lock ./Pipfile.lock

# Upgrade pip & pipenv to the latest version
RUN pip install --upgrade pip --index-url ${INDEXURL}
RUN pip install --upgrade pipenv --index-url ${INDEXURL}

# Install packages from Pipfile.lock
RUN pipenv install --deploy --system && pipenv --clear

# Set the PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:/opt/spark/work-dir"
#ENTRYPOINT [ "/opt/entrypoint.sh" ]