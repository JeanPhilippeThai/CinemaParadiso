""" Connect to bigquery and generate the python tables into BigQuery"""

from google.cloud import bigquery
import pandas as pd

import cristina_tables

def main():

    export GOOGLE_APPLICATION_CREDENTIALS="chemin/vers/ta_cle.json"
    bq_client = bigquery.Client()

    fa_general_movie_scores_by_genre_nogh = get_fa_general_movie_scores_by_genre_nogh(bq_client)
    fa_number_movies_per_genre_nogh = get_fa_number_movies_per_genre_nogh(bq_client)

    # TO DO: push in to bigquery
    
if __name__ == "__main__":
    main()