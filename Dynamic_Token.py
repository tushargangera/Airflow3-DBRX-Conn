from airflow.decorators import task
import requests

@task
def execute_api_call(token, **context):
    api_url = "https://your-service.com"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    response = requests.get(api_url, headers=headers)
    return response.json()

# Chaining the tasks in your DAG
retrieved_token = fetch_auth_token()
api_result = execute_api_call(token=retrieved_token)
