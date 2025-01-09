from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sqlite3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def categorize_complaints():
    with sqlite3.connect('/home/ec2-user/airflow/airflow.db') as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, description FROM complaints WHERE status = 'New'")
        complaints = cursor.fetchall()
        for complaint_id, description in complaints:
            # Simple categorization logic
            if 'delay' in description.lower():
                category = 'Service Delay'
            elif 'quality' in description.lower():
                category = 'Product Quality'
            else:
                category = 'General'
            cursor.execute("UPDATE complaints SET status = ?, category = ? WHERE id = ?",
                           ('Categorized', category, complaint_id))
        conn.commit()

def assign_complaints():
    with sqlite3.connect('/home/ec2-user/airflow/airflow.db') as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM complaints WHERE status = 'Categorized'")
        complaints = cursor.fetchall()
        for complaint_id, in complaints:
            # Simple assignment logic
            assignee = 'support@example.com'
            cursor.execute("UPDATE complaints SET status = ?, assignee = ? WHERE id = ?",
                           ('Assigned', assignee, complaint_id))
        conn.commit()

def track_resolution():
    with sqlite3.connect('/home/ec2-user/airflow/airflow.db') as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM complaints WHERE status = 'Assigned'")
        complaints = cursor.fetchall()
        for complaint_id, in complaints:
            # Simple resolution tracking logic
            resolution = 'Resolved'
            cursor.execute("UPDATE complaints SET status = ? WHERE id = ?",
                           (resolution, complaint_id))
        conn.commit()

def monitor_complaints():
    with sqlite3.connect('/home/ec2-user/airflow/airflow.db') as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 
                status, 
                COUNT(*) 
            FROM complaints 
            GROUP BY status
        ''')
        data = cursor.fetchall()
        print("Complaint State Counts:")
        for status, count in data:
            print(f"{status}: {count}")

with DAG('complaint_management', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    categorize = PythonOperator(
        task_id='categorize_complaints',
        python_callable=categorize_complaints
    )

    assign = PythonOperator(
        task_id='assign_complaints',
        python_callable=assign_complaints
    )

    track = PythonOperator(
        task_id='track_resolution',
        python_callable=track_resolution
    )

    monitor = PythonOperator(
        task_id='monitor_complaints',
        python_callable=monitor_complaints
    )

    categorize >> assign >> track >> monitor