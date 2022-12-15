try:
    from slack_sdk import WebClient
    from datetime import datetime
except Exception as e:
    print("Error : {}".format(e))

SLACK_CHANNEL_ID = "C04EPNAC31U"

client = WebClient(token="xoxb-4506243522051-4530076132272-LL4bSI9l2BXa8DYaRjWDyK9Q")
client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=f"Run successfully: {datetime.now()}")