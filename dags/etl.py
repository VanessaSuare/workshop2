''' Etl functions '''

import logging
import csv
import pandas as pd
import psycopg2
import sys
sys.path.append('/opt/airflow/dbconfig')
sys.path.append('../dbconfig/')
from dbconfig import configuration


def extract_spotify_data():
    '''Function to extract the data of spotify_dataset'''

    path = '/opt/airflow/data/spotify_dataset.csv'

    logging.info("Starting data extraction") 
    try:
        spotify_df = pd.read_csv(path)
    except pd.exceptions.RequestException as e:
        return f"Error: {e}"

    logging.info("Spotify data extraction finished")
    return spotify_df


def transform_spotify_data(spotify_data):
    '''Function to transform the spotify_data'''

    columns_to_drop = [
        'instrumentalness',
        'acousticness',
        'speechiness',
        'mode',
        'liveness',
        'key',
        'track_id',
    ]

    spotify_data.drop(columns=columns_to_drop, axis=1, inplace=True)
    logging.info("Transformations applied to Spotify data")

    return spotify_data


def load_spotify_data(spotify_data):
    ''' Function to load the spotify_data'''
    spotify_data.to_csv('/opt/airflow/outputs/spotify.csv', index=False)
    logging.info("Spotify data loaded into the output file")



def load_grammys_data():
    '''Function to extract and load the spotify_data'''

    connection = None
    try:
        params = configuration()
        logging.debug('Connecting to the postgreSQL database ...')
        with psycopg2.connect(**params) as connection:

            with connection.cursor() as crsr:

                crsr.execute('DROP TABLE IF EXISTS the_grammy_awards')

                create_table = """
                    CREATE TABLE IF NOT EXISTS the_grammy_award (
                        id SERIAL PRIMARY KEY,
                        year INT,
                        title TEXT,
                        published_at TIMESTAMP WITH TIME ZONE,
                        updated_at TIMESTAMP WITH TIME ZONE,
                        category TEXT,
                        nominee TEXT,
                        artist TEXT,
                        workers TEXT,
                        img TEXT,
                        winner BOOLEAN);"""
                crsr.execute(create_table)

                with open('../data/the_grammy_awards.csv', newline='', encoding="utf-8") as csv_file:
                    grammys_data = csv.reader(csv_file, delimiter=';')
                    for index, row in enumerate(grammys_data):
                        if index == 0:
                            continue
                        insert_data = crsr.mogrify("""
                            INSERT INTO the_grammy_awards (
                            year, title, published_at, updated_at, category, nominee, artist, workers, img, winner) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,  %s);""", row)
                        crsr.execute(insert_data)
                        logging.info('Grammys data successfully inserted into database!')
                        connection.commit()
                        return grammys_data
    except (ImportError, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            logging.debug('Database connection terminated.')


def merge(spotify_data, grammys_data):
    ''' Function to do the merge between spotify_data and grammys_data '''

    spotify_data.artists = spotify_data.artists.str.lower().str.split(';').str[0]
    grammys_data.artist = grammys_data.artist.str.lower()

    spotify_data.track_name = spotify_data.track_name.str.lower().str.replace(r"\(.*\)", "", regex=True)
    grammys_data.nominee = grammys_data.nominee.str.lower().str.replace(r"\(.*\)", "", regex=True)

    artists_merge = pd.merge(spotify_data, grammys_data, left_on='artists', right_on='artist', how='inner')
    track_name_merge = pd.merge(spotify_data, grammys_data, left_on='track_name', right_on='nominee', how='inner')

    final_merge = pd.concat([artists_merge, track_name_merge]).drop_duplicates()
    final_merge.to_csv('/opt/airflow/outputs/merge.csv', index=False)

    return final_merge
