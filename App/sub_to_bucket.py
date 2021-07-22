import logging
from base64 import b64decode
from json import loads
from random import randint
from pandas import DataFrame

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

    def transform_to_Dataframe(self, message: str) -> DataFrame:
        try:
            df = DataFrame(loads(message))
            if not df.empty:
                logging.info(f"Created DataFrame")
            else:
                logging.warning(f"empty dataframe")
            return df

        except Exception as e:
            logging.error(f"Error encounterered - str{e} ")
            raise


    def upload_df_to_bucket(self, df: DataFrame, file_name: str = "payload") -> None:
        storage_client = Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(f"{file_name}.csv")

        blob.upload_from_string(data=df.to_csv(index=False), content_type="txt/csv")

        logging.info(f"file uploaded to bucket")



def process(event, context):

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    obj = SubToBucket(event, context)

    data = obj.get_data()
    upload_df = obj.transform_to_Dataframe(data)
    obj.upload_df_to_bucket(upload_df, "data_file_"+str(randint(1000, 9999)))

        