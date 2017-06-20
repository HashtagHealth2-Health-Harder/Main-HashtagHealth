import errno
import sqlite3
conn = sqlite3.connect("health_tweets.db", check_same_thread = False)


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        global conn
        conn = sqlite3.connect(db_file)
        return conn
    except:
        print("\ncannot create connection\n")
    return None


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except:
        print("Could not create table")

def delete_table(conn, delete_table_sql):
    """ delete a table from the delete_table_sql statement
    :param conn: Connection object
    :param delete_table_sql: a DROP TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(delete_table_sql)
    except:
        print("Could not delete table")


def insert_tweet_main(tweet_id, location, datetime, profile_location, full_tweet, hashtags, mentions):
    global conn
    c = conn.cursor()
    c.execute(" INSERT INTO main(Tweet_ID, Location, DateTime, Full_Tweet, Hashtags, "
              "Mentions) VALUES (?, ?, ?, ?, ?, ?)", (tweet_id, location, datetime, full_tweet,
                                                      str(hashtags), str(mentions)))
    conn.commit()


def insert_tweet_tweets(tweet_id, full_tweet, topic, datetime, bio_location, location):
    global conn
    c = conn.cursor()
    c.execute("INSERT INTO tweets VALUES (NULL, ?, ?, ?, ?, ?, ?)", (tweet_id, full_tweet, topic, datetime,
                                                                     bio_location, location))
    conn.commit()


def insert_tweet_users(user_id, handle, region):
    global conn
    c = conn.cursor()
    c.execute("INSERT INTO users VALUES (NULL, ?, ?, ?)", (user_id, handle, region))
    conn.commit()


def insert_tweet_region(point, country, name, city):
    global conn
    c = conn.cursor()
    c.execute("INSERT INTO region VALUES (NULL, ?, ?, ?, ?)", (point, country, name, city))
    conn.commit()

def get_latest_tweet(): 
    global conn
    c = conn.cursor()
    result = c.execute("SELECT * FROM main ORDER BY DateTime DESC LIMIT 1")
    return result

def main():
    database = "C:\\sqlite\db\pythonsqlite.db"

    sql_create_main_table = " CREATE TABLE IF NOT EXISTS main (id INTEGER PRIMARY KEY, Tweet_ID INT NOT NULL, " \
                            "Location VARCHAR(25) NOT NULL,  DateTime DATETIME NOT NULL, " \
                            "Profile_location VARCHAR(25), Full_Tweet VARCHAR(150) NOT NULL, " \
                            "Hashtags TEXT, Mentions TEXT, BuzzWords TEXT ); "

    sql_create_region_table = """ CREATE TABLE IF NOT EXISTS region (
                                        id INTEGER PRIMARY KEY,
                                        Locale INT,
                                        Country VARCHAR(2),
                                        Full_Name VARCHAR(30),
                                        City VARCHAR(25)
                                        ); """

    sql_create_tweets_table = """ CREATE TABLE IF NOT EXISTS tweets (
                                        id INTEGER PRIMARY KEY,
                                        Tweet_ID INT NOT NULL,
                                        Tweet_Text TEXT NOT NULL,
                                        Topic VARCHAR(25),
                                        DateTime TEXT NOT NULL,
                                        Bio_Location VARCHAR(25),
                                        Tweet_Location VARCHAR(25)
                                        ); """

    sql_create_users_table = """ CREATE TABLE IF NOT EXISTS users (
                                        id INTEGER PRIMARY KEY,
                                        User_ID VARCHAR(25),
                                        Twitter_Handle VARCHAR(25),
                                        Region VARCHAR(25)
                                        ); """

    sql_create_tweeted_by_table = """ CREATE TABLE IF NOT EXISTS tweeted_by (
                                        id INTEGER PRIMARY KEY,
                                        User_ID VARCHAR(25),
                                        Tweet_ID INT NOT NULL
                                        ); """

    sql_create_trends_table = """ CREATE TABLE IF NOT EXISTS trends (
                                        id INTEGER PRIMARY KEY,
                                        Trend_Topic VARCHAR(25),
                                        DateTime INT NOT NULL,
                                        Region VARCHAR(30)
                                        ); """

    sql_create_categories_table = """ CREATE TABLE IF NOT EXISTS categories (
                                        id INTEGER PRIMARY KEY,
                                        Category VARCHAR(25)
                                        ); """

    sql_create_buzzwords_table = """ CREATE TABLE IF NOT EXISTS buzzwords (
                                        id INTEGER PRIMARY KEY,
                                        Buzzword VARCHAR(25)
                                        ); """

    sql_create_topics_table = """ CREATE TABLE IF NOT EXISTS topics (
                                        id INTEGER PRIMARY KEY,
                                        Category VARCHAR(25),
                                        Buzzword VARCHAR(25)
                                        ); """

    sql_delete_topics = """ DROP TABLE IF EXISTS topics; """
    sql_delete_buzzwords = """ DROP TABLE IF EXISTS buzzwords; """
    sql_delete_categories = """ DROP TABLE IF EXISTS categories; """
    sql_delete_trends = """ DROP TABLE IF EXISTS trends; """
    sql_delete_tweeted_by = """ DROP TABLE IF EXISTS tweeted_by; """
    sql_delete_users = """ DROP TABLE IF EXISTS users; """
    sql_delete_tweets = """ DROP TABLE IF EXISTS tweets; """
    sql_delete_region = """ DROP TABLE IF EXISTS region; """
    sql_delete_main = """ DROP TABLE IF EXISTS main; """

    # create a database connection
    conn = create_connection(database)
    if conn is not None:
        # delete all tables first
        delete_table(conn, sql_delete_topics)
        delete_table(conn, sql_delete_buzzwords)
        delete_table(conn, sql_delete_categories)
        delete_table(conn, sql_delete_trends)
        delete_table(conn, sql_delete_tweeted_by)
        delete_table(conn, sql_delete_users)
        delete_table(conn, sql_delete_tweets)
        delete_table(conn, sql_delete_region)
        delete_table(conn, sql_delete_main)

        # create table
        create_table(conn, sql_create_main_table)
        create_table(conn, sql_create_region_table)
        create_table(conn, sql_create_tweets_table)
        create_table(conn, sql_create_users_table)
        create_table(conn, sql_create_tweeted_by_table)
        create_table(conn, sql_create_trends_table)
        create_table(conn, sql_create_categories_table)
        create_table(conn, sql_create_buzzwords_table)
        create_table(conn, sql_create_topics_table)

    else:
        print("Error! cannot create the database connection.")

if __name__ == '__main__':
    main()
