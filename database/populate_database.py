from database import database_proxy
import pickle
import sys
import os

def load_file(topic, file_name):
    tweets = pickle.load(open('../../data/{}/{}'.format(topic, file_name), 'rb'))
    return tweets

def populate_data(tweet, topic):
    Tweet_ID = tweet.id
    Location = tweet.place.full_name  # AKA Tweet_Location
    DateTime = tweet.created_at
    Full_Tweet = tweet.text # Gabby i dont like this it's shorter than the full text
    Hashtags = tweet.entities["hashtags"]
    Mentions = tweet.entities["user_mentions"]
    Topic = topic  
    Region = str(tweet.place.bounding_box.coordinates)  
    Bio_Location = tweet.user.location  # AKA Profile_location
    User_ID = tweet.user.id
    Twitter_Handle = tweet.user.screen_name
    Exact_Location = str(tweet.coordinates)
    Country = tweet.place.country_code
    City = tweet.place.name

    database_proxy.insert_tweet_main(Tweet_ID, Location, DateTime, Bio_Location, Full_Tweet, Hashtags, Mentions)
    database_proxy.insert_tweet_tweets(Tweet_ID, Full_Tweet, Topic, DateTime, Bio_Location, Location)
    database_proxy.insert_tweet_users(User_ID, Twitter_Handle, Region)
    database_proxy.insert_tweet_region(Exact_Location, Country, Location, City)

def main(topic):
    # this data needs to go into the database
    # we need it in main just in case
        # ID, Location, DateTime, Full_Tweet, Hashtags, Mentions
    # we need it in tweets
        # ID, Tweet_Text, Topic(foreign key), DateTime, Bio_Location, Tweet_Location
    # we need it in users
        # User_ID, Twitter_Handle, Region(foreign key)
    # we need it in all related tables
        # Region, Topics, Buzzwords, Categories, Tweeted_By, Trends
    files = [f for f in os.listdir('../../data/{}'.format(topic))]
    for f in files:
        tweet = load_file(topic, f)
        populate_data(tweet, topic)

if __name__ == '__main__':
    # topic
    main(sys.argv[1])
