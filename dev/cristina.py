import pandas as pd

def get_fa_general_movie_scores_by_genre_nogh(client, query):

  # Read table cleaned_imdb_movies
  df_movie_scores = client.query(query).to_dataframe()

  # create a list with all the genres
  genres_list = ['genre_action', 'genre_adventure', 'genre_animation',
        'genre_comedy', 'genre_crime', 'genre_documentary', 'genre_drama',
        'genre_family', 'genre_fantasy', 'genre_history', 'genre_horror',
        'genre_musical', 'genre_music', 'genre_mystery', 'genre_romance',
        'genre_scifi','genre_thriller', 'genre_war',
        'genre_western', 'genre_biography', 'genre_film_noir']

  # Do a for loop to access all the genres in the list genres_list.
  # In each iteration, gropu by the corresponding genre (only where the value is 1) and calculate the mean of the weighted score.
  # In each iretarion, add list with the mean weithed score and as the key we store the genre. We add every iteratio in the created empty dictionary.
  dict = {}

  for genres in genres_list:
    genre_ratings_score = df_movie_scores.groupby(genres)["weighted_score"].mean().loc[1,]
    dict[genres.replace("genre_","",1)] = [genre_ratings_score]

  # transform the dictionary into a dataframe with column names
  df_movie_scores_by_genre = pd.DataFrame(dict).transpose().reset_index().rename(columns={"index":"genre",0:"weighted_score"})

  return df_movie_scores_by_genre

# NOT WORKING SO COMMENTED
# def get_fa_number_movies_per_genre_nogh(client, query):
#     # Read table cleaned_imdb_movies
#     df_movie_scores = client.query(query).to_dataframe()

#     # Number of movies per genre: agregate each of the genres to sum the 1
#     df_number_movies_per_genre = df_movie_scores["genres_list"].sum()
#     df_number_movies_per_genre = df_number_movies_per_genre.reset_index()
#     df_number_movies_per_genre.columns = ["genre", "number_of_movies"]
#     df_number_movies_per_genre = df_number_movies_per_genre.sort_values(by="number_of_movies", ascending=False)

#     return df_number_movies_per_genre