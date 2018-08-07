FROM ubuntu:latest
RUN apt-get update && apt-get install -y software-properties-common
RUN apt-get install -y python3-pip python3-dev build-essential
RUN pip3 install --upgrade pip
RUN apt-get install -y gdal-bin libgdal-dev python3-gdal
RUN apt-get install -y libssl-dev libffi-dev libcurl4-openssl-dev
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get install -y python3-tk

ADD ./webapp/requirements.txt /tmp/requirements.txt

# Install dependencies
RUN pip install -r /tmp/requirements.txt

# Add our code
ADD ./webapp /opt/webapp/
WORKDIR /opt/webapp

# Run the app.  CMD is required to run on Heroku.  $PORT is set by Heroku.
#CMD gunicorn --bind 0.0.0.0:$PORT wsgi --timeout 6000 

CMD python3 app.py