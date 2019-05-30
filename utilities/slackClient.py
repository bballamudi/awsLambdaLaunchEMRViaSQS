from slackclient import SlackClient
import os

#Setup Slack Client
# _SLACK_TOKEN = os.environ["SLACK_TOKEN"]
slackClient = SlackClient(
    client_id=,
    client_secret='',
    token='')


request = slackClient.api_call(
    "chat.postMessage"
    , channel="job_failures"
    , text="There was an issue with your Hadoop Job.")

print(request)
