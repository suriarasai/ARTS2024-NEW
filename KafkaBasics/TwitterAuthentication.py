import tweepy
import time

# Twitter API credentials
api_key = "SU5ytsPiPz1dCbDij0faLi8ww"
api_secret_key = "KLcsx4WiMp2Wi24JpJTDZYJ81YuB1697SCv1zThbyKjG0r94oH"
access_token = "322712118-jakcRFAZaD5i1LKRb5WEprKZ3sd6td7XtzTXfXt4"
access_token_secret = "2byzIwPQr86uKvSlSV8LniwPpcIS8V7rUqUxLnQyDV65m"
bearer_token = "AAAAAAAAAAAAAAAAAAAAACFGuAEAAAAA6lGRxhHqicIKx4g1f7ujnkJJ4kk%3D5MVy1MR7Cg06MLxk6rbaQTHjEsuD9Ph42Izm3wHGQt6wC2xVc4"

# Authenticate to Twitter
auth = tweepy.OAuthHandler(api_key, api_secret_key)
auth.set_access_token(access_token, access_token_secret)
# Create API object
api = tweepy.API(auth)

print("Authentication OK")
client = tweepy.Client(bearer_token, api_key, api_secret_key, access_token, access_token_secret)


# User's username to fetch tweets from
username = '@ScienceGuys'  # Replace 'twitter_username' with the actual username

# Fetch the latest 10 tweets from the user's timeline
tweets = api.user_timeline(screen_name=username, count=10)

# Print each tweet's text
for tweet in tweets:
    print(tweet.text)