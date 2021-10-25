FROM python:3.10

ENV CONTAINER_HOME=/var/www

ADD . ${CONTAINER_HOME}
WORKDIR ${CONTAINER_HOME}

RUN pip install -r ${CONTAINER_HOME}/requirements.txt
RUN export FLASK_APP=run.py
RUN export FLASK_ENV=production

CMD flask run --host=0.0.0.0
