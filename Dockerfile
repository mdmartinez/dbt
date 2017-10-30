FROM python:3.6

RUN apt-get update

RUN apt-get install apt-transport-https
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/8/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update

RUN apt-get install -y python-pip netcat
RUN apt-get install -y python-dev python3-dev
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql
RUN apt-get install unixodbc-dev

RUN pip install pip --upgrade
RUN pip install virtualenv
RUN pip install virtualenvwrapper
RUN pip install tox

WORKDIR /usr/src/app
RUN cd /usr/src/app
