from datetime import timedelta
from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago
import pandas as pd

default_args = {
            'owner' : 'Da Bi',
            'start_date' : days_ago(0),
            'email' : ['smash@gmail.com'],
            'email_on_failure' : 'True',
            'email_on_retry' : 'True',
            'retries' : 1,
            'retry_delay' : timedelta(minutes=5)
}

dag = DAG(
        'ETL_toll_data',
        schedule_interval = timedelta(days = 1),
        default_args = default_args,
        description = 'Apache Airflow Final Assignment',
        
)



def extract_data_from_csv():
    
    csv = pd.read_csv("/opt/airflow/processed_data/vehicle-data.csv", header=None)
    csv.columns = ['Rowid','Timestamp','Anonymized Vehicle number','Vehicle type','Number of axles','Vehicle code']
    csv = csv[['Rowid','Timestamp','Anonymized Vehicle number','Vehicle type']]
    csv.to_csv("/opt/airflow/processed_data/csv_data.csv", index=False)

def extract_data_from_tsv():
    
    tsv = pd.read_csv("/opt/airflow/processed_data/tollplaza-data.tsv", delimiter='\t', header=None)
    tsv.columns = ['Rowid','Timestamp','Anonymized Vehicle number','Vehicle type','Number of axles','Tollplaza id','Tollplaza code']
    tsv = tsv[['Number of axles','Tollplaza id','Tollplaza code']]
    tsv.to_csv("/opt/airflow/processed_data/tsv_data.csv", index=False)


def extract_data_from_fixed_width():
    
    column_widths = [(0, 6), (6, 31), (31, 40), (40, 48), (48, 58), (58, 62), (62, 68)] 
    fwf = pd.read_fwf("/opt/airflow/processed_data/payment-data.txt", colspecs=column_widths, header=None)
    
    fwf.columns = ['Rowid','Timestamp','Anonymized Vehicle number','Tollplaza id','Tollplaza code','Type of Payment code','Vehicle Code']
    fwf = fwf[['Type of Payment code','Vehicle Code']]
    fwf.to_csv("/opt/airflow/processed_data/fixed_width_data.csv", index=False)



# def consolidate_data():
    
#     csv = pd.read_csv("/opt/airflow/processed_data/csv_data.csv")
#     tsv = pd.read_csv("/opt/airflow/processed_data/tsv_data.csv")
#     fwf = pd.read_csv("/opt/airflow/processed_data/fixed_width_data.csv")
    
#     result = pd.concat([csv, tsv, fwf], axis=1)
#     result.to_csv("/opt/airflow/processed_data/newfile.csv", index=False)


unzip_data = BashOperator(
            task_id = 'unzip_data',
            bash_command = 'tar xzvf  "/opt/airflow/processed_data/tolldata.tgz" -C "/opt/airflow/processed_data"',
            dag=dag,

)

extract_data_from_csv = PythonOperator(
                    task_id = 'extract_data_from_csv',
                    python_callable = extract_data_from_csv,
                    dag = dag,
)



extract_data_from_tsv = PythonOperator(
                    task_id = 'extract_data_from_tsv',
                    python_callable = extract_data_from_tsv,
                    dag = dag,
)



extract_data_from_fixed_width = PythonOperator(
                    task_id = 'extract_data_from_fixed_width',
                    python_callable = extract_data_from_fixed_width,
                    dag = dag,
)

# consolidate_data = PythonOperator(
#                     task_id = 'consolidate_data',
#                     python_callable = consolidate_data,
#                     dag = dag,
# )

consolidate_data = BashOperator(
                    task_id = 'consolidate_data',
                    bash_command = 'paste -d"," "/opt/airflow/processed_data/csv_data.csv" "/opt/airflow/processed_data/tsv_data.csv" "/opt/airflow/processed_data/fixed_width_data.csv"  > "/opt/airflow/processed_data/RESULTAT.csv" ',
                    dag = dag,
)


transform_data = BashOperator(
                    task_id = 'transform_data',
                    bash_command = ' cut -d "," -f 1-3 "/opt/airflow/processed_data/RESULTAT.csv" > "/opt/airflow/processed_data/file1.csv" &&\
                                     cut -d "," -f 5-9 "/opt/airflow/processed_data/RESULTAT.csv" > "/opt/airflow/processed_data/file4.csv" &&\
                                     cut -d "," -f 4 "/opt/airflow/processed_data/RESULTAT.csv" |\
                                     tr [:lower:] [:upper:] > "/opt/airflow/processed_data/file2.csv" &&\
                                     paste -d"," "/opt/airflow/processed_data/file1.csv" "/opt/airflow/processed_data/file2.csv" "/opt/airflow/processed_data/file4.csv" > "/opt/airflow/processed_data/file3"',
                    dag = dag,
)


#pipeline

unzip_data >> [extract_data_from_csv,extract_data_from_tsv,extract_data_from_fixed_width] >> consolidate_data >> transform_data