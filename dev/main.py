"""Connect to BigQuery and generate the python tables into BigQuery"""
import cristina
import logging
import os
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

if not os.path.exists("logs"):
    os.makedirs("logs")

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_filename = f"logs/app_{timestamp}.log"

logging.basicConfig(
    level=logging.INFO,  
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("main code")


def main():
    client = _connect_to_bigquery()
    dataset_id = "cinema_paradiso"

    # Create the table fa_general_movie_scores_by_genre
    query_general = "SELECT * FROM `cinemaparadiso-462409.cinema_paradiso.fa_list_statistics_imdb_merged_bygenre`"
    df_general = cristina.get_fa_general_movie_scores_by_genre_nogh(client, query_general)
    table_id = "fa_general_movie_scores_by_genre_nogh"
    _send_dataframe_to_bigquery(client, df_general, dataset_id, table_id)

    # NOT WORKING SO COMMENTED
    # # Create the table fa_number_movies_per_genre_nogh
    # query_number = "SELECT * FROM `cinemaparadiso-462409.cinema_paradiso.fa_list_statistics_imdb_merged_bygenre`"
    # df_number = cristina.get_fa_number_movies_per_genre_nogh(client, query_number)
    # table_id = "fa_number_movies_per_genre_nogh"
    # _send_dataframe_to_bigquery(client, df_number, dataset_id, table_id)  


def _connect_to_bigquery():
    """
    Connect to BigQuery and return the client
    """
    logger.info("Connecting to BigQuery")

    credentials_path = os.getenv('GCP_CINEMAPARADISO_CREDENTIALS')
    
    if not credentials_path:
        raise ValueError("La variable GCP_CINEMAPARADISO_CREDENTIALS= n'est pas définie")

    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    logger.info("Successful connection")
    return client


def _send_dataframe_to_bigquery(client, df, dataset_id, table_id, write_disposition='WRITE_TRUNCATE'):
    """
    Send pandas DataFrame to BigQuery.
    
    Args:
        client: BigQuery client
        df (pd.DataFrame): DataFrame to send
        dataset_id (str): ID of targeted BigQuery dataset
        table_id (str): ID of targeted BigQuery table
        write_disposition (str): 'WRITE_TRUNCATE' (default), 'WRITE_APPEND' or 'WRITE_EMPTY'
    """
    # Complete destination table
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    logger.info(f"Sending data to {table_ref} ...")
    
    # Configuration for sending data
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        # Removed schema parameter as it wasn't defined and let BigQuery auto-detect
    )
    
    # Sending data
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    
    logger.info(f"Data sent successfully to {table_ref}")


if __name__ == "__main__":
    main()