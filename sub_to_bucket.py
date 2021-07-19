import logging
from base64 import b64decode
from json import loads
from random import randint

from google.cloud.storage import Client

class SubToBucket:
    def __init__(self, event, context):
        self.event = event
        self.context = context
        self.bucket_name = "golden-union-319717-test1"

    def get_data(self):
        logging.info(
            f"This function was triggered by message ID {self.context.event_id} published at {self.context.timestamp}"
            f"to {self.context.resource['name']}"
        )


        if "data" in self.event:
            pubsub_data = b64decode(self.event["data"]).decode("utf-8")
            logging.info(pubsub_data)
            return pubsub_data

        else:
            logging.error("Incorrect format")
            return ""


    def upload_df_to_bucket(self, data: str, file_name: str = "payload") -> None:
        storage_client = Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(f"{file_name}.txt")

        blob.upload_from_string(data=str(data), content_type="txt")

        logging.info(f"file uploaded to bucket")



def process(event, context):

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    obj = SubToBucket(event, context)

    upload_data = obj.get_data()

    obj.upload_df_to_bucket(upload_data, "data_file_"+str(randint(1000, 9999)))

        