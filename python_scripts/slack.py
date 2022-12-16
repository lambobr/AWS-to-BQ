try:
    from slack_sdk import WebClient
    from datetime import datetime
except Exception as e:
    print("Error : {}".format(e))

SLACK_CHANNEL_ID = "C04FBCV2YGK"

client = WebClient(token="INSERT SLACK ROBOT TOKEN HERE")
client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=f"Run successfully: {datetime.now()}")
