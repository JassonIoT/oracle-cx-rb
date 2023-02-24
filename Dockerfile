FROM python:3.9

RUN apt-get update && \
    apt-get install -y curl \
    ca-certificates gnupg2 \
    unixodbc unixodbc-dev \
    freetds-dev freetds-bin tdsodbc apt-utils

## Adding MSSQL dependencies
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list

## Accepting EULA for MSSQL drivers
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y --allow-unauthenticated msodbcsql17
RUN ACCEPT_EULA=Y apt-get install -y --allow-unauthenticated mssql-tools
RUN pip install -U pip setuptools wheel

## Copying application code
WORKDIR /code
COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
COPY ./apps /code/apps

RUN export PATH="$PATH:/opt/mssql-tools/bin" >> ~/.bash_profile && \
    export PATH="$PATH:/opt/mssql-tools/bin" >> ~/.bashrc
ENV PYTHONPATH "${PYTHONPATH}:/code/apps"

## Execute application entry point
CMD ["uvicorn", "apps.urls:apps", "--host", "0.0.0.0", "--port", "8080"]