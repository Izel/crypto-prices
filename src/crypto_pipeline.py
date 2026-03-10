import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv("../.env")
PROJECT_ID = os.getenv("PROJECT_ID")
PIPELINE_RUNNER = os.getenv("PIPELINE_RUNNER")
REGION = os.getenv("REGION")
SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID")
DATASET = os.getenv("DATASET")
TABLE = os.getenv("TABLE")
TEMP_BUCKET_PATH = os.getenv("TEMP_BUCKET_PATH")


class ProcessCryptoData(beam.DoFn):
    def process(self, element):
        import json
        from datetime import datetime

        # Parse the JSON data
        message = json.loads(element.decode("utf-8"))

        # Extract relevant fields
        yield {
            "symbol": message.get("symbol"),
            "price": float(message.get("price")),
            "event_time": message.get("timestamp"),
        }


def run():
    # Define pipeline options
    # options = PipelineOptions(
    #     project=PROJECT_ID,
    #     runner=PIPELINE_RUNNER,
    #     temp_location=TEMP_BUCKET_PATH,
    #     region=REGION,
    # )
    options = PipelineOptions(streaming=True)
    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromPubSub"
            >> ReadFromPubSub(
                subscription=f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}"
            )
            | "ProcessCryptoData" >> beam.ParDo(ProcessCryptoData())
            | "DefaultWindow" >> beam.WindowInto(window.FixedWindows(60))
            | "WriteToBigQuery"
            >> WriteToBigQuery(
                table=f"{PROJECT_ID}:{DATASET}.{TABLE}",
                schema="symbol:STRING, price:FLOAT, event_time:TIMESTAMP",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == "__main__":
    run()
