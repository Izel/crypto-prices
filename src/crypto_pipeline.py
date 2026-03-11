import apache_beam as beam

# from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from dotenv import load_dotenv
import json
import logging
from datetime import datetime
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
BQ_DEAD_LETTER_TABLE = os.getenv("BQ_DEAD_LETTER_TABLE")


class ProcessCryptoData(beam.DoFn):
    # Define a tag for dead-letter data
    DEAD_LETTER_TAG = "dead_letter"

    def process(self, element):
        try:
            # Parse the JSON data
            message = json.loads(element.decode("utf-8"))

            # Extract and format relevant fields
            yield {
                "trade_id": message.get("trade_id"),
                "symbol": message.get("product_id"),
                "price": float(message.get("price")),
                "operation": message.get("side"),
                "event_time": datetime.fromisoformat(message.get("time")),
            }
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON: {e} | Data: {element}")

        # Yield to a 'dead letter' tag for future review
        except Exception as e:
            yield beam.pvalue.TaggedOutput(
                self.DEAD_LETTER_TAG,
                {
                    "raw_data": str(element),
                    "error": str(e),
                    "error_timestamp": datetime.utcnow().isoformat(),
                },
            )


def run():
    options = PipelineOptions(streaming=True)
    with beam.Pipeline(options=options) as p:
        # Reads and formats data
        results = (
            p
            | "ReadFromPubSub"
            >> ReadFromPubSub(
                subscription=f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}"
            )
            | "ProcessCryptoData"
            >> beam.ParDo(ProcessCryptoData()).with_outputs(
                ProcessCryptoData.DEAD_LETTER_TAG, main="main_output"
            )
        )
        # Writes the good data to BQ
        (
            results.main_output
            | "WriteToBigQuery"
            >> WriteToBigQuery(
                table=f"{PROJECT_ID}:{DATASET}.{TABLE}",
                schema="symbol:STRING, price:FLOAT, event_time:TIMESTAMP, trade_id:STRING, operation:STRING",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )
        # Handles the broken data in a dead letter table in BQ
        (
            results[ProcessCryptoData.DEAD_LETTER_TAG]
            | "WriteToBigQueryDeadLetter"
            >> WriteToBigQuery(
                table=f"{PROJECT_ID}:{DATASET}.{BQ_DEAD_LETTER_TABLE}",
                schema="raw_data:STRING, error:STRING, error_timestamp:TIMESTAMP",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == "__main__":
    run()
