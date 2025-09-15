# NCOA Database Manager
This is a program to maintain an internal database of people associated with addresses, referencing the TrueNCOA API service to udpate people's address statuses as inactive when NCOA provides this data. The database is managed in a personal Google Cloud Project, using Google BigQuery and Google Cloud Storage.

The next stage of this project would be to split the script into modular phases (connecting to DB, calling TrueNCOA API, updating DB) and create this as tasks within a DAG orchestrated within Airflow.

## How to Run
First, activate a virtual environment and install the requirements:
```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Next, you will need to have a credentialed google cloud project service account key to the GCP project I created, saved locally with the filepath updated in `config.py`.

Then, to run an update of the database–either for the first time or updating–run the file ncoa_request.py:
```
python ncoa_request.py
```

## The data
This is using North Carolina Board of Elections data for the core reference internal database (public and real data). I've not been able to get a TrueNCOA api key, so there's a script I wrote to generate a sample response based on TrueNCOA's docs.
