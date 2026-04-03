from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta

# 1. CẤU HÌNH MẶC ĐỊNH
default_args = {
    'owner': 'lam-tran',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='crypto_ecosystem_pipeline_gke',
    default_args=default_args,
    start_date=datetime(2025, 9, 11),
    schedule='@hourly', # Chạy mỗi ngày 1 lần (có thể đổi thành @hourly)
    catchup=False,
    tags=['crypto', 'production', 'gke'],
) as dag:

    # 2. TASK 1: GỌI AIRBYTE
    # Airflow sẽ bắn một tín hiệu HTTP sang Airbyte và ngồi đợi nó báo "Success"
    trigger_airbyte = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_cmc',
        airbyte_conn_id='airbyte-connections', # Tên Connection ta sẽ tạo trên Airflow UI
        connection_id='2be387bc-7396-48c4-b323-38e6b2eefd26', # Thay bằng ID UUID trên thanh URL của Airbyte
        asynchronous=False, # Bắt buộc là False để Airflow chờ Airbyte chạy xong mới đi tiếp
    )

    # 3. TASK 2: CHẠY DBT STAGING
    dbt_run_staging = KubernetesPodOperator(
        task_id='dbt_run_staging',
        name='dbt-staging-pod',       
        namespace='airflow',          
        image='lamtran911/dbt-crypto-pipeline:bcc1ec1', 
        image_pull_policy='Always',   # <-- THÊM DÒNG NÀY: Ép kéo image mới nhất từ Docker Hub
        cmds=["dbt", "run", "--select", "staging", "--profiles-dir", "."], 
        is_delete_operator_pod=True,  
        in_cluster=True,              
        get_logs=True,
        service_account_name='dbt-ksa', 
    )

    # 4. TASK 3: CHẠY DBT MARTS (Mô hình hóa Star Schema)
    dbt_run_marts = KubernetesPodOperator(
        task_id='dbt_run_marts',
        name='dbt-marts-pod',
        namespace='airflow',
        image='lamtran911/dbt-crypto-pipeline:bcc1ec1',
        image_pull_policy='Always',   # <-- THÊM DÒNG NÀY
        cmds=["dbt", "run", "--select", "marts", "--profiles-dir", "."],
        is_delete_operator_pod=True,
        in_cluster=True,
        get_logs=True,
        service_account_name='dbt-ksa',
    )

    # 5. ĐỊNH NGHĨA LUỒNG CHẠY (DAG Dependencies)
    trigger_airbyte >> dbt_run_staging >> dbt_run_marts