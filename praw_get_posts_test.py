import praw
import time
import datetime
import json

import gspread
from google.oauth2 import service_account
import pandas as pd

# Load credentials from config.json
with open('config.json', 'r') as config_file: #store credential as local .json
      config = json.load(config_file)
      reddit_credentials = config["reddit_credentials"]
      googlesheet_credentials = config["googlesheet_credentials"]


# Initialize the Reddit API client
reddit = praw.Reddit(
        client_id=reddit_credentials["client_id"],
        client_secret=reddit_credentials["client_secret"],
        user_agent=reddit_credentials["user_agent"])

subreddit = reddit.subreddit('CanTax')

 # Connect to the spreadsheet
def load_googlesheet(spreadsheetname="reddit_cantax",tabname = "posts"):
  scope = ['https://spreadsheets.google.com/feeds',
              'https://www.googleapis.com/auth/drive']
  credentials = service_account.Credentials.from_service_account_info(googlesheet_credentials, scopes = scope)

  # Create a gspread client
  client = gspread.Client(credentials)
  spread = client.open(spreadsheetname)
  worksheet = spread.worksheet(tabname)
  return worksheet

worksheet = load_googlesheet()

# Function to scrape and append posts
def scrape_posts():
    counter = 0
    after_token = None
    after_token_set = set()  # To keep track of seen `after` tokens

    while True:
        try:

            titles = []
            authors = []
            scores = []
            bodies = []
            timestamps = []
            permalinks = []

            counter +=1
            # Get the next set of posts using the `after` parameter
            next_posts = subreddit.new(limit=25, params={'after': after_token})
            new_posts_found = False  # Flag to check if new posts are found #TODO: incorrect exit

            # Write a batch of reddit posts
            for post in next_posts:
                if post.fullname not in after_token_set:
                    titles.append(post.title)
                    authors.append(post.author.name if post.author else '[deleted]')
                    scores.append(post.score)
                    bodies.append(post.selftext)

                    # Convert the created_utc timestamp to a datetime object
                    #created_datetime = datetime.datetime.fromtimestamp(post.created_utc)
                    timestamps.append(post.created_utc)

                    permalinks.append('https://www.reddit.com' + post.permalink)
                    after_token = post.fullname  # Store the ID of the last post retrieved
                    after_token_set.add(after_token)  # Add to the set
                    new_posts_found = True 

                    print('Scraped post:', post.title)

            # Create a Pandas DataFrame
            data = {
                "titles": titles,
                "authors": authors,
                "scores": scores,
                "bodies":bodies,
                "timestamps":timestamps,
                "permalinks":permalinks
            }
            df = pd.DataFrame(data)

            # Convert the DataFrame to a list of lists
            data_to_append = df.values.tolist()

            # Add new records
            worksheet.append_rows(data_to_append)

            # Sleep 10 min for every 32 batches of posts (800 posts)
            if counter % 32 ==0:
              time.sleep(600)
              print("Sleeping for 10 min to avoid time-out...")

            # If no new posts are found, exit the loop
            if not new_posts_found:
                print('No new posts found. Exiting.')
                break

        except Exception as e:
            print('An error occurred:', str(e))
            # Handle exceptions, e.g., sleep and retry if rate-limited
            time.sleep(60)  # Sleep for a minute

# Call the function to start scraping
scrape_posts()
