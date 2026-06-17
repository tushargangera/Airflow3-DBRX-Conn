import os, boto3, json, inspect, ast
from datetime import datetime
from airflow.sdk import DAG, task, Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
 
# Global Variables setup
def set_global_variables():
    global app_env_name, dpe_id
    print("In "+inspect.stack()[0][3]+" function" )
    app_env_name = os.environ["AIRFLOW_VAR_APP_ENV_NAME_LOWER"]
    dpe_id = os.environ["AIRFLOW_VAR_DPE_ID_LOWER"]
 
# Retrieve secret from secrets manager
def get_secrets_from_sm(secret_name):
    print("In "+inspect.stack()[0][3]+" function" )
    print("Inside get_secrets_from_sm")
    print("Secrets name is {0}".format(secret_name))
    print("App Env Name is {0}".format(app_env_name))
    LAMBDA_CLIENT = boto3.client('lambda',region_name='us-east-1')
 
    # DEV: 784022468640:function:dpe0014-devprd-sec-api
    # UAT: 062240745513:function:dpe0014-uatprd-sec-api
    # PRD: 222200269700:function:dpe0014-prdprd-sec-api
    if app_env_name.upper() == "DEV":
        SECRET_API_ARN = "arn:aws:lambda:us-east-1:784022468640:function:dpe0014-devprd-sec-api"
    elif app_env_name.upper() == "UAT":
        SECRET_API_ARN = "arn:aws:lambda:us-east-1:062240745513:function:dpe0014-uatprd-sec-api"
    elif app_env_name.upper() == "PRD":
        SECRET_API_ARN = "arn:aws:lambda:us-east-1:222200269700:function:dpe0014-prdprd-sec-api"
    else:
        print("Incorrect Environment specified. No SECRET_API_ARN found")
        exit(1)
 
    # Get session token from IAM Role
    session = boto3.Session(region_name='us-east-1')
    credentials = session.get_credentials()
    current_credentials = credentials.get_frozen_credentials()
    secret_name_prefix = dpe_id+'-'+app_env_name+'-'
    secret_full_name = secret_name_prefix+secret_name
    print("Secret Full Name for {0} is requested".format(secret_full_name))
 
    # Form the payload
    request_payload = {
        "ApiVersion": 1,
        "Action": "Get",
        "Name": secret_full_name,
        "Owner": {
            "AccessKeyId": current_credentials.access_key,
            "SecretAccessKey":current_credentials.secret_key,
            "SessionToken": current_credentials.token
        }
    }
    os.environ["AccessKeyId"] = current_credentials.access_key
    os.environ["SecretAccessKey"] = current_credentials.secret_key
    os.environ["SessionToken"] = current_credentials.token
 
    # Trigger Lambda and get response
    response = LAMBDA_CLIENT.invoke(
        FunctionName=SECRET_API_ARN,
        Payload=json.dumps(request_payload),
        # Payload=json.dumps(request_payload_create),
        InvocationType="RequestResponse"
    )
    response_payload = json.loads(response['Payload'].read().decode("utf-8"))
    print(response_payload)
 
 
   
    response_status = response_payload["ResponseMetadata"]["Status"]
    print("Response Status - {0}".format(response_status))
 
    if response_status == 'SUCCESS':
        print("Response Status is successful")
        response_value = response_payload["Value"]
        print("The Response Value is {0}".format(response_value))
        return(response_value)
    else:
        print("ERROR:Response Status is not successful")
        print(response_payload)
        exit(1)
 
def create_ssh_key_file(connect_id):
    global priv_key_file_full_nm, airflow_user_id
    print("In "+inspect.stack()[0][3]+" function" )
    airflow_user_id_key = "AIRFLOW_VAR_"+connect_id.upper()+"_USER_ID"
    airflow_user_id =os.environ[airflow_user_id_key]
    print("The airflow user id is {0}".format(airflow_user_id))
    priv_key_file_nm=connect_id+airflow_user_id+"_priv.pem"
    priv_key_file_full_nm = '/tmp/'+priv_key_file_nm
    print("Private Key file name is {0}".format(priv_key_file_full_nm))
    Variable.set('priv_key_file_nm','/tmp/'+priv_key_file_nm)
    os.chdir('/tmp')
    print("The current working directory is {0}".format(os.getcwd()))
    if os.path.isfile(priv_key_file_nm):
        print("SSH Private key file for connection {0} already exist".format(connect_id))
        dir_list = os.listdir('/tmp')
        print("Dir list is {0}".format(dir_list))
                   
    else:
        print("SSH Private key file for connection {0} doesn't exist".format(connect_id))
        print("Creating SSH private key file for {0}".format(connect_id))
        secret_name_key="AIRFLOW_VAR_"+connect_id.upper()+"_KEY_SECRETS_NM"
        print("The secret name key is "+secret_name_key)
        secret_name = os.environ[secret_name_key]
        print(secret_name)
        secrets_value = get_secrets_from_sm(secret_name)
        print("secrets value is {0}".format(secrets_value))
        priv_key_file = open(priv_key_file_nm,'w')
        priv_key_file.write(secrets_value.strip())
        priv_key_file.close()
        os.chmod(priv_key_file_nm, 0o600)
        dir_list = os.listdir('/tmp')
        print("Dir list is {0}".format(dir_list))
 
def remove_ssh_key_file(connect_id):
    print("In "+inspect.stack()[0][3]+" function" )
    priv_key_file_nm=connect_id+airflow_user_id+"_priv.pem"
    os.chdir('/tmp')
    print("The current working directory is {0}".format(os.getcwd()))
    if os.path.isfile(priv_key_file_nm):
        print("SSH Private key file for connection {0} already exist".format(connect_id))
        os.remove(priv_key_file_nm)
        print("The SSH key file is removed")
        dir_list = os.listdir('/tmp')
        print("Dir list after SSH key removal is {0}".format(dir_list))
 
 
def run_bash_cmd(connect_id):
    print("In "+inspect.stack()[0][3]+" function" )
    os.listdir('/usr/local/airflow/dags')
    os.listdir('/tmp')

def create_connection_via_api(conn_id, conn_type, host=None, login=None, password=None, port=None, schema=None, description=None, extra=None):
    """
    Prepare connection payload for Airflow 3 REST API
    Returns a dictionary ready to be sent via HttpOperator
    """
    print("In "+inspect.stack()[0][3]+" function" )
    
    connection_data = {
        'connection_id': conn_id,
        'conn_type': conn_type,
    }
    
    if host:
        connection_data['host'] = host
    if login:
        connection_data['login'] = login
    if password:
        connection_data['password'] = password
    if port:
        connection_data['port'] = port
    if schema:
        connection_data['schema'] = schema
    if description:
        connection_data['description'] = description
    if extra:
        connection_data['extra'] = extra
    
    return connection_data
 
def prepare_ssh_conn_payload(**kwargs):
    """Prepare SSH connection payload from secrets"""
    print("In "+inspect.stack()[0][3]+" function" )
    set_global_variables()
    global priv_key_file_full_nm, airflow_user_id
    connect_id = kwargs['connect_id']
    connect_id_name = connect_id+'_ssh_conn'
    host_name = connect_id+'_host'
    user_id = connect_id+'_user_id'
    airflow_user_id_key = "AIRFLOW_VAR_"+connect_id.upper()+"_USER_ID"
    airflow_user_id =os.environ[airflow_user_id_key]
    print("The airflow user id is {0}".format(airflow_user_id))
    priv_key_file_nm=connect_id+airflow_user_id+"_priv.pem"
    priv_key_file_full_nm = '/tmp/'+priv_key_file_nm
    Variable.set('priv_key_file_nm','/tmp/'+priv_key_file_nm)
    print("Environment Name:"+app_env_name)
    print("Connect Id :"+connect_id_name)
    remove_ssh_key_file(connect_id)
    create_ssh_key_file(connect_id)
    
    # Build SSH connection payload for REST API
    payload = create_connection_via_api(
        conn_id=connect_id_name,
        conn_type='ssh',
        host=Variable.get(host_name),
        login=Variable.get(user_id),
        password='',
        port=22,
        description='SSH Connection to '+connect_id,
        extra=json.dumps({"key_file": priv_key_file_full_nm})
    )
    return payload

def prepare_dbricks_conn_payload(**kwargs):
    """Prepare Databricks connection payload from secrets"""
    print("In "+inspect.stack()[0][3]+" function" )
    set_global_variables()
    connect_id = kwargs['connect_id']
    connect_id_name = connect_id+'_dbricks_conn'
    dbricks_secret_name = 'databricks-application-'+os.environ["AIRFLOW_VAR_DBRICKS_KEY_SECRETS_NM"]
    print("Environment Name:"+app_env_name)
    print("Connect Id :"+connect_id_name)
    print("Databricks Secrets Name:"+dbricks_secret_name)
    token_response = get_secrets_from_sm(dbricks_secret_name)
    token_val = ast.literal_eval(token_response)["token_value"]
    print("The Secret is {0}".format(token_val))
    host_val=Variable.get("dbricks_host_url")+".cloud.databricks.com"
    print("The host name of databricks is {0}".format(host_val))
    
    # Build Databricks connection payload for REST API
    payload = create_connection_via_api(
        conn_id=connect_id_name,
        conn_type='databricks',
        host=host_val,
        password=token_val,
        description='Databricks connection -'+connect_id
    )
    return payload
 
 
default_args = {
    'owner': 'airflow'
}

with DAG(dag_id='setup',
        start_date=None,
        schedule=None,
        catchup=False,
        default_args=default_args,
        tags=["api", "infrastructure", "airflow3"]
        ) as dag:

    # Task 1: Prepare SSH connection payload
    prepare_ssh_task = PythonOperator(
        task_id='prepare_ssh_conn_payload',
        python_callable=prepare_ssh_conn_payload,
        op_kwargs={'connect_id': 'psdlec2'},
        do_xcom_push=True
    )

    # Task 2: Create SSH connection via Airflow 3 REST API
    create_ssh_conn_task = HttpOperator(
        task_id='post_ssh_connection',
        method='POST',
        endpoint='api/v2/connections',
        http_conn_id='airflow_api_server_conn',
        data='{{ ti.xcom_pull(task_ids="prepare_ssh_conn_payload") | tojson }}',
        headers={
            'Content-Type': 'application/json',
        },
        extra_options={'check_response_errors': False},
        log_response=True,
    )

    # Task 3: Prepare Databricks connection payload
    prepare_dbricks_task = PythonOperator(
        task_id='prepare_dbricks_conn_payload',
        python_callable=prepare_dbricks_conn_payload,
        op_kwargs={'connect_id': 'psdl'},
        do_xcom_push=True
    )

    # Task 4: Create Databricks connection via Airflow 3 REST API
    create_dbricks_conn_task = HttpOperator(
        task_id='post_dbricks_connection',
        method='POST',
        endpoint='api/v2/connections',
        http_conn_id='airflow_api_server_conn',
        data='{{ ti.xcom_pull(task_ids="prepare_dbricks_conn_payload") | tojson }}',
        headers={
            'Content-Type': 'application/json',
        },
        extra_options={'check_response_errors': False},
        log_response=True,
    )

    # Set task dependencies
    prepare_ssh_task >> create_ssh_conn_task
    prepare_dbricks_task >> create_dbricks_conn_task  