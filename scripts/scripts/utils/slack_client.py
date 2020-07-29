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

def send_error(channel, title, message=None, trace=None):
    if not client: return None
    title = f"Error: {title}"
    if trace: text += f"\n```{trace}```"
    return client.chat_postMessage(
        channel=channel,
        attachments=[
            {
                "color": ERROR_COLOR,
                "title": title,
                "fallback": title,
                "text": message
            }
        ]
    )

def send_warning(channel, title, message=None, trace=None):
    if not client: return None
    title = f"Warning: {title}"
    if trace: text += f"\n```{trace}```"
    return client.chat_postMessage(
        channel=channel,
        attachments=[
            {
                "color": WARNING_COLOR,
                "title": title,
                "fallback": title,
                "text": message
            }
        ]
    )

def send_success(channel, title, message=None, trace=None):
    if not client: return None
    if trace: text += f"\n```{trace}```"
    return client.chat_postMessage(
        channel=channel,
        attachments=[
            {
                "color": SUCCESS_COLOR,
                "title": title,
                "fallback": title,
                "text": message
            }
        ]
    )

if __name__ == '__main__':
    send_error(
        channel='bot-testing',
        title='Random error',
        message='Abc\ndef\nghi'
    )
    send_warning(
        channel='bot-testing',
        title='Random error',
        message='Abc\ndef\nghi'
    )
    send_success(
        channel='bot-testing',
        title='Random error'
    )
