import pandas as pd

# Read table cleaned_imdb_movies
query = "SELECT * FROM `cinemaparadiso-462409.cinema_paradiso.fa_list_statistics_imdb_merged_bygenre`"
df_movie_scores = client.query(query).to_dataframe()


# Number of movies per genre: agregate each of the genres to sum the 1
df_number_movies_per_genre = df_movie_scores[genres_list].sum()
df_number_movies_per_genre = df_number_movies_per_genre.reset_index()
df_number_movies_per_genre.columns = ["genre", "number_of_movies"]
df_number_movies_per_genre = df_number_movies_per_genre.sort_values(by="number_of_movies", ascending=False)

# create a csv file
csv_filename = 'fa_number_movies_per_genre_nogh'
a_number_movies_per_genre_nogh.to_csv(csv_filename, index=False)
