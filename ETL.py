import pandas as pd
import sqlite3
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'weather_history_etl_noapi1',
    default_args=default_args,
    description='ETL pipeline for weather history dataset',
    schedule_interval='@daily',
)

file_directory = '/Users/rimjim/airflow/datasets/'
db_path = '/Users/rimjim/airflow/databases/weatherHistory_data.db'
csv_file_path = '/Users/rimjim/airflow/datasets/weatherHistory.csv'

#TAsk 1 : Extract Data

def extract_data(**kwargs):
    #Authenticate and download the 'Weather Dataset'
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files("muthuj7/weather-dataset", file_directory)

    # # Check if the downloaded file is a ZIP file
    zip_path = file_directory + 'weather-dataset.zip'
    if zipfile.is_zipfile(zip_path):
        with zipfile.ZipFile(zip_path) as Zip:
            Zip.extractall(file_directory)
    else: 
        raise ValueError('Downloaded file is not a zip file. Aborting extraction.')

    # Xcom to pass file path to next step in pipeline
    kwargs['ti'].xcom_push(key='file_path', value=file_path)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

def transform_data(**kwargs):
    csv_path = kwargs['ti'].xcom_pull(key='file_path')
    df = pd.read_csv(csv_path)

    # Convert 'Formatted Date' to datetime
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'], errors='coerce', utc=True)
    df.drop_duplicates(inplace= True)
    
    # Handle missing values in critical columns
    critical_columns = ['Temperature (C)', 'Humidity', 'Wind Speed (km/h)', 'Visibility (km)', 'Pressure (millibars)']
    df[critical_columns].fillna(df[critical_columns].median(), inplace=True)

    # Extract month from 'Formatted Date'
    df['date'] = df['Formatted Date'].dt.date
    df['month'] = df['Formatted Date'].dt.month
    
    # Calculate daily averages for critical features
    daily_agg = df.groupby('date').agg({
        'Temperature (C)': 'mean',
        'Humidity': 'mean',
        'Wind Speed (km/h)': 'mean',
    }).reset_index()

    # Calculate monthly mode for 'Precip Type', if there's no mode, set to NA
    monthly_mode = df.groupby('month')['Precip Type'].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else pd.NA).reset_index()
    monthly_mode.columns = ['month', 'Mode']

    # Add wind_strength column categorizing wind speed
    def wind_strength(speed):
        if speed <= 1.5:
            return 'Calm'
         elif speed <= 3.3:
            return 'Light Air'
         elif speed <= 5.4:
            return 'Light Breeze'
         elif speed <= 7.9:
            return 'Gentle Breeze'
         elif speed <= 10.7:
            return 'Moderate Breeze'
         elif speed <= 13.8:
            return 'Fresh Breeze'
         elif speed <= 17.1:
            return 'Strong Breeze'
         elif speed <= 20.7:
            return 'Near Gale'
         elif speed <= 24.4:
            return 'Gale'
         elif speed <=28.4:
            return 'Strong Gale'
         elif speed <= 32.6:
            return 'Storm'
        else:
            return 'Violent Storm'

    #Apply eind strength categorization to the dataset
    df['wind_strength'] = df['Wind Speed (km/h)'].apply(wind_strength)
    # Merge wind strength with the daily aggregates
    daily_agg = daily_agg.merges(
        df[['date', 'Formatted Dete', 'wind_strength']].drop_duplicates(),
        on='date',
        how='left'
    )

    # Calculate monthly averages for critical features
    monthly_agg = df.groupby('month').agg({
        'Temperature (C)': 'mean',
        'Humidity': 'mean',
        'Wind Speed (km/h)': 'mean',
        'Visibility (km)': 'mean',
        'Pressure (millibars)': 'mean'
        'Precip Type': lambda x: x.mode()[0] if not x.mode().empty else None
    }).reset_index()

    # Save daily and monthly transformed data to new CSV files
    daily_path = '/Users/rimjim/airflow/tmp/daily_transformed.csv'
    monthly_path = '/Users/rimjim/airflow/tmp/monthly_transformed.csv'
    daily_agg.to_csv(cdaily_path, index=False)
    monthly_agg.to_csv(monthly_path, index=False)

    # Push the file paths to XCom
    kwargs['ti'].xcom_push(key='daily_path', value=daily_path)
    kwargs['ti'].xcom_push(key='monthly_path', value=monthly_path)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

def validate_data(**kwargs):
    # Load the transformed data paths from XCom
    daily_path = kwargs['ti'].xcom_pull(key='daily_path', task_ids='transform_data')
    monthly_path = kwargs['ti'].xcom_pull(key='monthly_path',  task_ids='transform_data)

    df_daily = pd.read_csv(daily_path)
    df_monthly = pd.read_csv(monthly_path)

    # Define ranges for validation
    temperature_range = (-50, 50)  # Reasonable temperature range
    humidity_range = (0, 1)  # Humidity as a proportion (0 to 1)
    wind_speed_min = 0  # Wind speed should be >= 0

    # --- Validate Daily Data ---
    # Missing values check
    print('DATA: '+ str(daily_df))
    if daily_df.isnull().any().any():
        raise ValueErroe("Daily data contains missing values.")

    # Range checks for daily data
    if not daily_df['Temperature (C)'].between(*temperature_range).all():
         raise ValueErroe("Daily Temperature values are out of range.")
    if not daily_df['Humidity'].between(*humidity_range).all():
         raise ValueErroe("Daily Humidity values are out of range.")
    if not (daily_df['Wind Speed (km/h)'] >= wind_speed_min).all():
         raise ValueErroe("Daily Wind Speed values are out of range.")

    # --- Validate Monthly Data ---
    # Missing values check
    if monthly_df.isnull().any().any():
         raise ValueErroe"Monthly data contains missing values.")

    # Range checks for monthly data
    if not monthly_df['Temperature (C)'].between(*temperature_range).all():
         raise ValueErroe("Monthly Temperature values are out of range.")
    if not monthly_df['Humidity'].between(*humidity_range).all():
         raise ValueErroe("Monthly Humidity values are out of range.")
    if not (monthly_df['Wind Speed (km/h)'] >= wind_speed_min).all():
         raise ValueErroe("Monthly Wind Speed values are out of range.")

    # --- Outlier Detection ---
    # Define outlier thresholds for temperature (adjust as needed for context)
    temp_outlier_thresholds = (-30, 40)  # Seasonal reasonable limits
    daily_temp_outliers = daily_df[~daily_df['Temperature (C)'].between(*temp_outlier_thresholds)]
    if not daily_temp_outliers.empty:
         raise ValueErroe(f"Daily Temperature outliers detected: {daily_temp_outliers['Temperature (C)'].values}")

    monthly_temp_outliers = monthly_df[~monthly_df['Temperature (C)'].between(*temp_outlier_thresholds)]
    if not monthly_temp_outliers.empty:
         raise ValueErroe(f"Monthly Temperature outliers detected: {monthly_temp_outliers['Temperature (C)'].values}")

    print("Validation passed: All checks succeeded for daily and monthly data.")

# Task definition for validation
validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    provide_context=True,
    dag=dag,
)

def load_data(**kwargs):
    # Retrieve transformed file paths from XCom
    daily_path = kwargs['ti'].xcom_pull(key='daily_path', task_ids='transdorm_data')
    monthly_path = kwargs['ti'].xcom_pull(key='monthly_path', task_ids='transdorm_data')

    # Load the data from the Csv files
    daily_df = pd.read_csv(daily_path)
    monthly_df = pd.read_csv(monthly_df)

    # Rename the dataframe to match the SQL table properties
    daily_df = daily_df.drop(columns=['date'])
    daily_df = daily_df.rename(
        colums={
            'Temperature (C) : 'temperature_c',
            'Formatted Date' : 'formatted_date',
            'Humidity' : 'humidity',
            'Wind Speed (km/h)' : 'wind_speed_kmh'
        }
    )

    monthly_df = monthly_df.drop(columns=['Wind Speed (km/h)'])
    daily_df = daily_df.rename(
        colums={
            'Temperature (C) : 'avg_temperature_c',
            'Formatted Date' : 'formatted_date',
            'Humidity' : 'avg_humidity',
            'Visibility (km)' : 'avg_visibilty_km',
            'Pressure (millibars)' : 'avg_pressure_millibars',
            'Precip Type' : 'mode_precip_type'
        }
    )

    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create daily_weather table if it doesn't exist
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            formatted_date TEXT,
            temperature_c REAL,
            apparent_temperature_c REAL,
            humidity REAL,
            wind_speed_kmh REAL,
            visibility_km REAL,
            pressure_millibars REAL,
            wind_strength TEXT,
            avg_temperature_c REAL,
            avg_humidity REAL,
            avg_wind_speed_kmh REAL
        )
     ''')

    # Create monthly_weather table if it doesn't exist
    cursor.execute('''
         CREATE TABLE IF NOT EXISTS monthly_weather (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             month INTEGER,
             avg_temperature_c REAL,
             avg_apparent_temperature_c REAL,
             avg_humidity REAL,
             avg_visibility_km REAL,
             avg_pressure_millibars REAL,
             mode_precip_type TEXT
         )
     ''')


    # Insert the data into the respective tables
    daily_df.to_sql('daily_weather', conn, if_exists='append', index=False)
    monthly_df.to_sql('monthly_weather', conn, if_exists='append', index=False)

    conn.close()

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    trigger_rule = 'all_success',
    dag=dag,
)

# Set task dependencies
extract_task >> transform_data >> validate_data >> load_data
