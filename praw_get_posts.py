#Good copy
import asyncpraw
import pandas as pd
from datetime import datetime
import nest_asyncio

nest_asyncio.apply()

async def fetch_reddit_data(subreddit_name, post_limit):

    # Load credentials from config.json
    with open('config.json', 'r') as config_file: #store credential as local .json
      config = json.load(config_file)
      reddit_credentials = config["reddit_credentials"]

    # Authenticate your Reddit app
    reddit = asyncpraw.Reddit(
        client_id=reddit_credentials["client_id"],
        client_secret=reddit_credentials["client_secret"],
        user_agent=reddit_credentials["user_agent"],
        username=reddit_credentials["username"],
        password=reddit_credentials["password"]
    )

    # Fetch the subreddit
    subreddit = await reddit.subreddit(subreddit_name)

    # Create lists to store extracted information
    titles = []
    authors = []
    scores = []
    bodies = []
    timestamps = []
    permalinks = []

    # Retrieve posts from the subreddit
    async for post in subreddit.new(limit=post_limit):
        titles.append(post.title)
        # authors.append(post.author.name if post.author else '[deleted]')
        scores.append(post.score)
        bodies.append(post.selftext)
        timestamps.append(datetime.fromtimestamp(post.created_utc))
        permalinks.append('https://www.reddit.com' + post.permalink)

    # Create a DataFrame from the extracted information
    data = {
        'Title': titles,
        # 'Author': authors,
        'Score': scores,
        'Body': bodies,
        'Timestamp': timestamps,
        'Permalink': permalinks
    }

    df = pd.DataFrame(data)

    return df

# Within your asyncio event loop context

async def main():
    # Run the asynchronous fetch_reddit_data function
    df = await fetch_reddit_data('CanTax',100000)

    # Print the DataFrame
    print(df)

    df.to_csv('reddit_posts.csv', index=False)
    print("DataFrame saved to 'reddit_posts.csv'")

# Create and run the event loop
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
