import unittest
from unittest.mock import MagicMock
from plugins.slack import SlackAlert 

class TestSlackAlert(unittest.TestCase):

    def setUp(self):
        self.token = "test_slack_token"
        self.channel = "#test_channel"
        self.slack_alert = SlackAlert(channel=self.channel, token=self.token)
        self.mock_context = {
            "task_instance": {
                "task_id": "test_task",
                "dag_id": "test_dag",
                "execution_date": "2023-08-15T00:00:00",
                "log_url": "http://example.com/logs",
            }
        }

    def test_fail_alert(self):
        self.slack_alert._send_alert = MagicMock()
        self.slack_alert.fail_alert(self.mock_context)
        self.slack_alert._send_alert.assert_called_with(self.mock_context, 'Failed')

    def test_success_alert(self):
        self.slack_alert._send_alert = MagicMock()
        self.slack_alert.success_alert(self.mock_context)
        self.slack_alert._send_alert.assert_called_with(self.mock_context, 'Success')

if __name__ == '__main__':
    unittest.main()