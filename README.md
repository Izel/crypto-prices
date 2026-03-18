# Real-Time Crypto Data Pipeline (GCP)

This project demonstrates a production-style real-time streaming pipeline on **GCP**. It ingests live cryptocurrency market data via **Pub/Sub**, processes it using Apache Beam on **Dataflow** (including dead-letter handling for malformed events), and persists structured data to **BigQuery** for analytics via **Looker Studio**  
The system is built using **Google Cloud Platform managed services** and follows a modern streaming data architecture.

---

## Architecture Overview

The pipeline processes streaming messages produced by crypto price events (Buy and Sell) in real time.

### Data flow:

Crypto Stream Producer  
→ Pub/Sub  
→ Dataflow (Apache Beam pipeline)  
→ BigQuery  
→ Analytics / SQL queries

<img src="assets/img/architecture.png" alt="Architecture diagram" width="500" height="400">

---

## Technologies Used

* Python
* Apache Beam
* Google Cloud Dataflow
* Google Cloud Pub/Sub
* Google BigQuery
* Google Cloud Storage
* Looker Studio

---

## Data Pipeline

The pipeline performs the following steps:

1. **Ingest streaming data** from Pub/Sub
2. **Parse JSON messages**
3. **Validate and transform events** 
4. **Write structured data to BigQuery**

Example of received event:   
``` JSON
{
"type":"ticker",
"sequence":123894075370,
"product_id":"BTC-USD",
"price":"70691.27",
"open_24h":"71290.91",
"volume_24h":"10302.66386023",
"low_24h":"68980.74",
"high_24h":"71485",
"volume_30d":"314531.69968052",
"best_bid":"70691.27",
"best_bid_size":"0.05851373",
"best_ask":"70691.28",
"best_ask_size":"0.01890000",
"side":"sell",
"time":"2026-03-11T16:54:02.163093Z",
"trade_id":978694534,
"last_size":"0.001705"
}
```
Example of the data after preprocesing (DoFn)
``` JSON
{
"trade_id":978694534,
"symbol": "BTC-USD", 
"price": 64210.12, 
"operation": "sell",
"timestamp": "2026-03-10T14:00:00Z"
}
```
---

## BigQuery Table Schema

| Field | Type |
|-----|-----|
| trade_id | STRING |
| symbol | STRING |
| price | FLOAT |
| operation | STRING |
| event_time | TIMESTAMP |
| processing_time | TIMESTAMP |

---

## Running the Pipeline

### Install dependencies

`pip install -r requirements.txt`

### Application Key

1. Crate a SA to avoid using your personal account email to run the pipeline.
2. Add the role `bb` to run the Dataflow Service Account
3. Generate a JSON key and store it in your local machine
4. Create an enviroment variable `GOOGLE_APPLICATION_CREDENTIALS` with your JSON path.

``` bash
export GOOGLE_APPLICATION_CREDENTIALS=<KEY_TO_JSON_FILE>  # For Linux systems
```

### Run locally

`python crypto_pipeline.py --runner DirectRunner`

### Deploy to Dataflow
Use the command below to submit the pipeline to cloud.  Provide a name to easily identify your pipeline job by replacing `PIPELINE_NAME`, otherwise, Dataflow will set a random unfriendly name. You can use the same bucket for `--temp_location` and `--staging_location` but create different folders for each one.
``` bash
python crypto_pipeline.py \                                                                                                         
 --runner DataflowRunner \
 --project <YOUR_PROJECT_ID> \
 --region <YOUR_REGION> \
 --temp_location <YOUR_BUCKET>/temp \
 --staging_location <YOUR_BUCKET>/staging \
 --max_num_workers 1 \
 --worker_machine_type e2-standard-2 
 --job_name=<PIPELINE_NAME>
```
> [!IMPORTANT]
> The error *ZONE_RESOURCE_POOL_EXHAUSTED* is a common error related to availability of resources in the selected region. Try to run the pipeline in a [different zone](https://docs.cloud.google.com/compute/docs/regions-zones), even if it is different to the zone chosen for the project, but not too far, at least not an intercontinental region, to avoid high charges. Other alternative is to use a different type of machine for `worker_machine_type` parameter. 

---

### Graphs
*The pipeline*\
<img src="assets/img/pipeline_graph.png" alt="The pipeline graph." width="370" height="300">

*The sink as a BQ table*\
![BQ table as sink](assets/img/sink.png)


*The dead letter sink as a BQ table*\
![BQ table for dead letter](assets/img/dead_letter.png)


---

# Analytics Dashboard

A ![real-time dashboard]("analytics/README.md") was created using **Looker Studio** to visualise cryptocurrency price trends. 

---

## Use Cases

This architecture can be used for:

* Real-time financial analytics
* Market monitoring
* Trading dashboards
* Streaming anomaly detection

---

## Future Improvements

Possible improvements to the pipeline:

* Add data validation :white_check_mark:
* Implement dead-letter queues :white_check_mark:
* Add monitoring and alerting :construction:
* Add Looker dashboard :white_check_mark:
* Add Terraform :construction:
