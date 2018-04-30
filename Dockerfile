FROM ubuntu:latest
RUN apt-get update && apt-get install -y software-properties-common
RUN apt-get install -y python-pip python-dev build-essential 
RUN apt-get install -y gdal-bin python-gdal python3-gdal
RUN apt-get install -y libssl-dev libffi-dev python-dev libcurl4-openssl-dev
RUN apt-get install -y python-tk

ADD ./webapp/requirements.txt /tmp/requirements.txt

# Install dependencies
RUN pip install -r /tmp/requirements.txt

# Add our code
ADD ./webapp /opt/webapp/
WORKDIR /opt/webapp	

# Run the app.  CMD is required to run on Heroku.  $PORT is set by Heroku.
# CMD gunicorn --bind 0.0.0.0:$PORT wsgi --timeout 6000

CMD python app.py
