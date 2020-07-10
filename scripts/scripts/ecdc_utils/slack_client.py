import os
from dotenv import load_dotenv
from slack import WebClient
from slack.errors import SlackApiError

load_dotenv()

SLACK_API_TOKEN = os.getenv('SLACK_API_TOKEN')
client = WebClient(token=SLACK_API_TOKEN) if SLACK_API_TOKEN else None

ERROR_COLOR = "#a30200"
WARNING_COLOR = "#f2c744"
SUCCESS_COLOR = "#01a715"

def send_error(channel, message, trace=None):
    if not client: return None
    text = f"*Error*: {message}"
    if trace: text += f"\n```{trace}```"
    return client.chat_postMessage(
        channel=channel,
        attachments=[
            {
                "color": ERROR_COLOR,
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": text
                        }
                    }
                ]
            }
        ]
    )

def send_warning(channel, message, trace=None):
    if not client: return None
    text = f"*Warning*: {message}"
    if trace: text += f"\n```{trace}```"
    return client.chat_postMessage(
        channel=channel,
        attachments=[
            {
                "color": WARNING_COLOR,
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": text
                        }
                    }
                ]
            }
        ]
    )

def send_success(channel, message, trace=None):
    if not client: return None
    text = message
    if trace: text += f"\n```{trace}```"
    return client.chat_postMessage(
        channel=channel,
        attachments=[
            {
                "color": WARNING_COLOR,
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": text
                        }
                    }
                ]
            }
        ]
    )

# if __name__ == '__main__':
#     try:
#         response = send_error(
#             'corona-data-updates',
#             'Random error',
#             'Abc\ndef\nghi'
#         )
#     except SlackApiError as e:
#         # You will get a SlackApiError if "ok" is False
#         assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
