# envlope = {'message': {'data': 'eyJ2ZXJzaW9uIjoiMS4wIiwicGFja2FnZU5hbWUiOiJjb20uY3JlYXRlYWlhcnQuYWlnZW5lcmF0b3IuZHJhdy5waG90byIsImV2ZW50VGltZU1pbGxpcyI6IjE2OTIwOTA5OTgzNDIiLCJ0ZXN0Tm90aWZpY2F0aW9uIjp7InZlcnNpb24iOiIxLjAifX0=', 'messageId': '8181801065393541', 'message_id': '8181801065393541', 'publishTime': '2023-08-15T09:16:38.374Z', 'publish_time': '2023-08-15T09:16:38.374Z'}, 'subscription': 'projects/pc-api-5581123954802239973-637/subscriptions/AperoTrustedAppSubCriptions'}"
import base64

test = {'data': 'eyJ2ZXJzaW9uIjoiMS4wIiwicGFja2FnZU5hbWUiOiJjb20uY3JlYXRlYWlhcnQuYWlnZW5lcmF0b3IuZHJhdy5waG90byIsImV2ZW50VGltZU1pbGxpcyI6IjE2OTIwOTA5OTgzNDIiLCJ0ZXN0Tm90aWZpY2F0aW9uIjp7InZlcnNpb24iOiIxLjAifX0='}
print(test['data'])

decoded_bytes = base64.b64decode(test['data'])
decoded_string = decoded_bytes.decode('utf-8')
print(decoded_string)
