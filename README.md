# Real-Time Crypto Data Pipeline (GCP)

This project implements a **real-time data processing pipeline** that ingests cryptocurrency market data, processes it using a streaming pipeline, and stores it for analytics.

The system is built using **Google Cloud Platform managed services** and follows a modern streaming data architecture.

---

## Architecture Overview

The pipeline processes streaming crypto price events in real time.

Data flow:

Crypto Stream Producer  
→ Pub/Sub  
→ Dataflow (Apache Beam pipeline)  
→ BigQuery  
→ Analytics / SQL queries

---

## Technologies Used

* Python
* Apache Beam
* Google Cloud Dataflow
* Google Cloud Pub/Sub
* Google BigQuery
* Google Cloud Storage

---

## Repository Structure

│  
├── src  
│ ├── crypto_pipeline.py  
│ ├── transforms.py  
│ └── schemas.py  
│  
├── requirements.txt  
│  
├── architecture  
│ └── architecture.md  
│  
└── README.md  

---

## Data Pipeline

The pipeline performs the following steps:

1. **Ingest streaming data** from Pub/Sub
2. **Parse JSON messages**
3. **Validate and transform events**
4. **Write structured data to BigQuery**

Example event:   
``` json
{
"symbol": "BTC-USD", 
"price": 64210.12, 
"timestamp": "2026-03-10T14:00:00Z"
}
```
---

## BigQuery Table Schema

| Field | Type |
|-----|-----|
| symbol | STRING |
| price | FLOAT |
| event_time | TIMESTAMP |
| processing_time | TIMESTAMP |

---

## Running the Pipeline

### Install dependencies

`pip install -r requirements.txt`

### Run locally

`python crypto_pipeline.py --runner DirectRunner`

### Deploy to Dataflow
``` bash
python crypto_pipeline.py
--runner DataflowRunner
--project YOUR_PROJECT
--region  YOUR_REGION
--temp_location gs://YOUR_BUCKET/temp
--staging_location gs://YOUR_BUCKET/staging
```
---

## Example Analytics Query
``` SQL
SELECT
symbol, 
AVG(price) as avg_price
FROM crypto_prices
WHERE event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
GROUP BY symbol
```
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

* Add data validation
* Implement dead-letter queues
* Add monitoring and alerting
* Add ML-based anomaly detection
* Add Terraform
