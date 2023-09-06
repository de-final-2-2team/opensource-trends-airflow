from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class SlackAlert:
    def __init__(self, channel, token):
        self.channel = channel
        self.client = WebClient(token=token)

    def _send_alert(self, context, status):
        try:
            text=  f"""
                *Result* {status} :{"alert" if status == "Failed" else "checkered_flag:"}
                *Task*: {context.get('task_instance').task_id}  
                *Dag*: {context.get('task_instance').dag_id}
                *Execution Date*: {context.get('execution_date')}  
                *Log Url*: {context.get('task_instance').log_url}
                date : {datetime.today().strftime('%Y-%m-%d')}
                """
            result = self.client.chat_postMessage(channel=self.channel, text=text)
            logger.info(result)
        except SlackApiError as e:
            logger.error(f"Error sending message: {e}")

    def fail_alert(self, context):
        self._send_alert(context, 'Failed')

    def success_alert(self, context):
        self._send_alert(context, 'Success')