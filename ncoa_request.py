import uuid
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from config import BIGQUERY_SERVICE_ACCOUNT_FILE, BUCKETS_SERVICE_ACCOUNT_KEY
import logging
from google.cloud import storage


logger = logging.getLogger(__name__)


def init_clients_and_job():
    # Initialize BigQuery bq_client
    bq_client = bigquery.Client.from_service_account_json(BIGQUERY_SERVICE_ACCOUNT_FILE)
    buckets_client = storage.Client.from_service_account_json(
        BUCKETS_SERVICE_ACCOUNT_KEY
    )
    # Generate a unique job_id and current datetime
    job_id = str(uuid.uuid4())
    job_run_datetime = datetime.utcnow()
    logger.info(
        f"Successfully created BigQuery and Cloud Storage clients. Initiatived NCOA Job ID: {job_id}, Run Time (UTC): {job_run_datetime}"
    )
    return {
        "bq_client": bq_client,
        "buckets_client": buckets_client,
        "job_id": job_id,
    }


def query_full_person_db(bq_client):

    # Query the vf_nc_partial table
    source_table = "vr-mail-generator.voterfile.vf_nc_partial"
    query_limit = None
    query = f"""
    SELECT
        vr_program_id AS individual_id,
        first_name AS first_name,
        last_name AS last_name,
        mail_addr1 AS address_line_1,
        mail_addr2 AS address_line_2,
        mail_city AS address_city,
        'NC' AS address_state,
        mail_zipcode AS address_zipcode
    FROM `{source_table}`
    WHERE vr_program_id IS NOT NULL
    """
    if query_limit is not None:
        query += f"LIMIT {query_limit}"
    logger.debug(
        f"Executing query to fetch data from {source_table}. Full Query:\n\n{query}"
    )

    query_job = bq_client.query(query)
    results = query_job.result()
    logger.info(
        f"BigQuery person db query executed successfully. Retrieved {results.total_rows} rows from {source_table}."
    )
    # Convert query results to a DataFrame
    data = [
        {
            "individual_id": row.individual_id,
            "first_name": row.first_name,
            "last_name": row.last_name,
            "address_line_1": row.address_line_1,
            "address_line_2": row.address_line_2 or "",
            "address_city": row.address_city,
            "address_state": row.address_state,
            "address_zipcode": row.address_zipcode,
        }
        for row in results
    ]
    input_df = pd.DataFrame(data)
    return input_df


def save_input_file_to_bucket(input_df, buckets_client, job_id):
    # Save DataFrame to Google Cloud Storage bucket
    bucket_name = "ncoa_data"
    input_blob_name = f"ncoa_request_files/job_request_input_id_{job_id}.csv"
    bucket = buckets_client.bucket(bucket_name)
    input_blob = bucket.blob(input_blob_name)
    input_blob.upload_from_string(input_df.to_csv(index=False), content_type="text/csv")
    print(f"Input data saved to gs://{bucket_name}/{input_blob_name}")
    # ncoa_response = query_NCOA_data(input_df, test=True)


def query_NCOA_data(input_data, test=False):
    if test:
        # NOTE: This assumes no rows in our input data that do not exist in the sample test output data
        sample_output = pd.read_csv("sample_ncoa_output.csv")
        merged_data = input_data.merge(
            sample_output[["record_id", "record_type"]],
            how="left",
            left_on="individual_id",
            right_on="record_id",
        )
        filtered_sample_output_data = merged_data.drop(columns=["record_id"])
        return filtered_sample_output_data
    else:
        # TODO: Implement actual NCOA query logic here. I've not yet been approved for TrueNCOA access.
        return None


def save_out_file_to_bucket(response_df, buckets_client, job_id):
    # Save NCOA response to Google Cloud Storage bucket
    bucket_name = "ncoa_data"
    bucket = buckets_client.bucket(bucket_name)
    response_blob_name = f"ncoa_response_files/job_request_response_id_{job_id}.csv"
    response_blob = bucket.blob(response_blob_name)
    response_blob.upload_from_string(
        response_df.to_csv(index=False), content_type="text/csv"
    )
    print(f"NCOA response data saved to gs://{bucket_name}/{response_blob_name}")


def update_bq_with_ncoa_response(bq_client, ncoa_response):

    # Update the BigQuery table with the response data
    table_id = "vr-mail-generator.vr_data.ncoa_address_statuses"

    ncoa_response.rename(
        columns={
            "individual_id": "vr_program_id",
            "first_name": "first_name",
            "last_name": "last_name",
            "address_line_1": "address_line_1",
            "city_name": "address_city",
            "state_code": "address_state",
            "postal_code": "address_zipcode",
            "record_type": "ncoa_status",
        },
        inplace=True,
    )

    # Create a temporary table in BigQuery to store the response data
    temp_table_id = f"{table_id}_temp"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,  # <-- let BQ infer from df
    )
    bq_client.load_table_from_dataframe(
        ncoa_response, temp_table_id, job_config=job_config
    ).result()

    # Perform a single query to find matching and non-matching records
    query = f"""
    MERGE `{table_id}` AS target
    USING `{temp_table_id}` AS source
    ON target.vr_program_id = source.vr_program_id
    AND target.address_line_1 = source.address_line_1
    AND target.address_city = source.address_city
    AND target.address_zipcode = source.address_zipcode
    AND target.address_state = source.address_state
    WHEN MATCHED THEN
    UPDATE SET
    last_attempted_update = DATETIME(CURRENT_DATETIME()),
    ncoa_status = CASE
        WHEN target.ncoa_status != source.ncoa_status THEN source.ncoa_status
        ELSE target.ncoa_status
    END,
    last_updated = CASE
        WHEN target.ncoa_status != source.ncoa_status THEN DATETIME(CURRENT_DATETIME())
        ELSE target.last_updated
    END
    WHEN NOT MATCHED THEN
    INSERT (
    vr_program_id, address_line_1, address_city, address_zipcode, address_state,
    first_name, last_name, ncoa_status, last_updated, last_attempted_update
    )
    VALUES (
    source.vr_program_id, source.address_line_1, source.address_city, source.address_zipcode,
    source.address_state, source.first_name, source.last_name, source.ncoa_status,
    DATETIME(CURRENT_DATETIME()), DATETIME(CURRENT_DATETIME())
    )
    """
    bq_client.query(query).result()

    # Clean up the temporary table
    bq_client.delete_table(temp_table_id, not_found_ok=True)


def run_ncoa_process():
    # Initialize clients and job details
    init_data = init_clients_and_job()
    bq_client = init_data["bq_client"]
    buckets_client = init_data["buckets_client"]
    job_id = init_data["job_id"]

    # Step 1: Query the full person database from BigQuery
    input_df = query_full_person_db(bq_client)

    # Step 2: Save the input file to Google Cloud Storage bucket, format for TrueNCOA API
    save_input_file_to_bucket(input_df, buckets_client, job_id)

    # Step 3: Query NCOA data (using test data for now)
    ncoa_response = query_NCOA_data(input_df, test=True)

    # Step 4: Save the NCOA response to Google Cloud Storage bucket
    save_out_file_to_bucket(ncoa_response, buckets_client, job_id)

    # Step 5: Update BigQuery with the NCOA response data
    update_bq_with_ncoa_response(bq_client, ncoa_response)
