# Resize the training data for 10 min execution time.
# Emojis. Repeated words
# model doesn't understand sarcasm
import sqlite3
import re
import os


def sanitize(string):
    # We shall be using lowercase characters only for simplicity
    string = string.lower()

    # Remove all the '@' mentions, since they do not contribute in assessing the sentiment of the tweet.
    mentions = re.compile(r'@\w*')
    string = mentions.sub("", string)

    # Sometimes, especially in hashtags, the underscore is used in lieu of a space
    foo = re.compile(r'_')
    string = foo.sub(" ", string)

    # Remove all numeric characters
    foo = re.compile(r'[0-9]*')
    string = foo.sub("", string)

    # Remove anything that isn't an alpha-numeric character
    foo = re.compile(r'[^\w|\t| |]')
    string = foo.sub(' ', string)

    return string.split()  # It'd be nice if someone just handed over the words to us, wouldn't it?


def create_database(answer=None):
    # Remove any existing dictionary
    if 'dictionary.db' in os.listdir():
        if answer is None:
            answer = input("There is already a dictionary database in this directory. " +
                           "Are you sure you want to rewrite it? ('y/Y' to proceed; anything else to exit)")
        if answer.lower() == 'y': os.remove('dictionary.db')
        else:
            print("Cancelling action.")
            return

    # Create SQLite database
    conn = sqlite3.connect("dictionary.db")
    c = conn.cursor()
    c.execute('CREATE TABLE word_list (' +
              'id INT,' +
              'word VARCHAR,' +
              'pos_count INT,' +
              'neg_count INT)')

    c.execute('CREATE TABLE stats (' +
              'total_pos_words INT,' +
              'total_neg_words INT,' +
              'total_words INT,' +
              'pos_tweets INT,' +
              'neg_tweets INT)')
    c.execute("INSERT INTO stats VALUES (0, 0, 0, 0, 0)")

    conn.commit()
    print("Database created.\n")
    conn.close()


def update_database(path):
    # Checking for database.
    if 'dictionary.db' not in os.listdir():
        print()
        raise OSError("The database is not present. Create a database by the name of \'dictionary.db\' first...")
    conn = sqlite3.connect('dictionary.db')
    print("Database found.")
    c = conn.cursor()

    # Load the database and sanitize
    try:
        import pandas as pd
        database = pd.read_csv(path)
    except FileNotFoundError:
        print("The CSV file is not present in the local directory. Please place the file in the directory and retry.\n")
        conn.close()
        return

    try:
        database['SentimentText'] = database['SentimentText'].apply(sanitize)
        print("Sanitization complete.")

        c.execute('SELECT * from stats')
        prev_pos_count, prev_neg_count, prev_word_count, prev_pos_tweet_count, prev_neg_tweet_count = c.fetchone()
        word_index = prev_word_count
        pos_tweets = 0
        neg_tweets = 0
        tot_pos_word_count = 0
        tot_neg_word_count = 0
        batch_index = 1
        for tweet in database.iterrows():
            if tweet[1]['Sentiment'] == 0:
                neg_tweets += 1
            else:
                pos_tweets += 1
            for word in tweet[1]['SentimentText']:
                # Search for the word in the table
                c.execute("SELECT * FROM word_list where word = '{}'".format(word))

                # If the word is not present, then add it to
                # the dictionary, and set the respective sentiment counters.
                result = c.fetchone()
                if result is None:
                    if tweet[1]['Sentiment'] == 0:
                        c.execute("INSERT INTO word_list VALUES (%d, '%s', 0, 1)" % (word_index, word))
                        tot_neg_word_count += 1
                    else:
                        c.execute("INSERT INTO word_list VALUES (%d, '%s', 1, 0)" % (word_index, word))
                        tot_pos_word_count += 1
                    word_index += 1

                # Otherwise, just update the counter
                else:
                    pos_count = result[2]
                    neg_count = result[3]
                    if tweet[1]['Sentiment'] == 0:
                        neg_count += 1
                        tot_neg_word_count += 1
                    else:
                        pos_count += 1
                        tot_pos_word_count += 1
                    c.execute("UPDATE word_list SET pos_count = %d WHERE id = %d" % (pos_count, id))
                    c.execute("UPDATE word_list SET neg_count = %d WHERE id = %d" % (neg_count, id))

            # Committing after every 1000th tweet
            if (pos_tweets + neg_tweets) % 1000 == 0:
                print("Processed a batch of 1000 tweets(%d)." % batch_index)
                conn.commit()
                batch_index += 1
    except KeyError:
        print("\nThe CSV file is not properly formatted. Please ensure " +
              "that the columns are named 'ItemID', 'Sentiment'" +
              " and 'SentimentText'.")
        conn.close()
        return

    conn.commit()  # So that the last few entries do get saved, or in cases where we're dealing with <1000 tweets.

    prev_neg_count += tot_neg_word_count
    prev_pos_count += tot_pos_word_count
    prev_neg_tweet_count += neg_tweets
    prev_pos_tweet_count += pos_tweets
    c.execute("UPDATE stats SET total_pos_words = %d" % prev_pos_count)
    c.execute("UPDATE stats SET total_neg_words = %d" % prev_neg_count)
    c.execute("UPDATE stats SET total_words = %d" % word_index)
    c.execute("UPDATE stats SET pos_tweets = %d" % prev_pos_tweet_count)
    c.execute("UPDATE stats SET neg_tweets = %d" % prev_neg_tweet_count)
    conn.commit()

    print()
    print("Dictionary successfully updated.")
    print("%d words found." % (word_index - prev_word_count))
    print("%d positive words." % tot_pos_word_count)
    print("%d negative words." % tot_neg_word_count)
    print("%d tweets processed (%d positive, %d negative)." % (pos_tweets + neg_tweets, pos_tweets, neg_tweets))
    print()
    conn.close()


def classify(test_tweet):
    import numpy as np
    if "dictionary.db" not in os.listdir():
        print("The dictionary database isn't present in the local directory. Please create a database first.")
        return

    conn = sqlite3.connect('dictionary.db')
    words = sanitize(test_tweet)
    c = conn.cursor()
    array = np.zeros((len(words), 2))
    counter = 0

    # Fill in the words
    for word in words:
        try:
            c.execute("SELECT pos_count, neg_count FROM word_list WHERE word = '{}'".format(word))
        except sqlite3.OperationalError:
            print("\nThe dictionary database is in incorrect format or corrupted. Please recreate the database.")
            conn.close()
            return

        result = c.fetchone()
        if result is not None:
            pos, neg = result
            array[counter][0] = pos
            array[counter][1] = neg
        array[counter][0] += 1
        array[counter][1] += 1
        counter += 1

    c.execute("SELECT total_pos_words, total_neg_words, total_words FROM stats")
    pos_tweet_words, neg_tweet_words, total_words = c.fetchone()
    pos_tweet_words += total_words
    neg_tweet_words += total_words

    array[:, 0] = array[:, 0]/pos_tweet_words
    array[:, 1] = array[:, 1]/neg_tweet_words
    array = np.prod(array, axis=0)

    c.execute("SELECT pos_tweets, neg_tweets FROM stats")
    pos_tweets, neg_tweets = c.fetchone()

    array = array / (pos_tweets + neg_tweets)
    array[0] = array[0] * pos_tweets
    array[1] = array[1] * neg_tweets

    s = np.sum(array)
    array = array / s

    print("{}% chance that the tweet has a positive sentiment.".format(array[0]*100))
    print("{}% chance that the tweet has a negative sentiment.".format(array[1]*100))

    conn.close()

