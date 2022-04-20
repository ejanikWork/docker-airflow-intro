from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from airflow.models import Variable


def read_secret(client, path, key):
    read_response = client.secrets.kv.v2.read_secret_version(path=path)
    key_value_pair = read_response['data']['data']
    value = key_value_pair[key]
    return value


def send_message_to_slack(slack_client, text, channel):
    try:
        response = slack_client.chat_postMessage(channel=channel, text=text)
        assert response["message"]["text"] == text
    except SlackApiError as e:
        # You will get a SlackApiError if "ok" is False
        assert e.response["ok"] is False
        assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
        print(f"Got an error kek: {e.response['error']}")


def alert_to_slack(**kwargs):
    slack_token = Variable.get_variable_from_secrets("slack_token")

    slack_client = WebClient(token=slack_token)

    dag_id = kwargs['dag_id']
    exec_date = kwargs['execution_date']
    channel = kwargs['channel']
    text = "Dag: " + dag_id + " finished at " + exec_date
    send_message_to_slack(slack_client, text, channel)


