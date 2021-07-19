FROM python:3.9-slim-buster

COPY . /egen-module1-capstone

WORKDIR /egen-module1-capstone

RUN pip install -r requirements.txt

ENTRYPOINT ["python"]

CMD ["App/apidata_to_topic.py"]