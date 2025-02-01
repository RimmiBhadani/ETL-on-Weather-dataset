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

db_path = '/mnt/c/Users/pn002/Desktop/koulu/Golnaz/airflow/databases/weather_history_database.db'
csv_folder_path = '/mnt/c/Users/pn002/Desktop/koulu/Golnaz/airflow/datasets/'
csv_file_path = '/mnt/c/Users/pn002/Desktop/koulu/Golnaz/airflow/datasets/weatherHistory.csv'


def extract_data(**kwargs):

    # # Check if the downloaded file is a ZIP file
    # if zipfile.is_zipfile(zip_file_path):
    #     # If it's a ZIP file, unzip it
    #     with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    #         zip_ref.extractall(csv_folder_path)
    #     # Update file path to extracted CSV
    #     os.remove(zip_file_path)  # Optionally delete the ZIP file after extraction
    # else:
    #     print("Downloaded file is not a ZIP archive, skipping extraction.")
    kwargs['ti'].xcom_push(key='weather_data', value=csv_file_path)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

def transform_data(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='weather_data')
    df = pd.read_csv(file_path)

    # Convert 'Formatted Date' to datetime
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'], errors='coerce', utc=True)

    # Handle missing values in critical columns
    critical_columns = ['Temperature (C)', 'Humidity', 'Wind Speed (km/h)', 'Visibility (km)', 'Pressure (millibars)']
    df[critical_columns].fillna(df[critical_columns].median(), inplace=True)

    # Drop duplicates
    df.drop_duplicates(inplace=True)

    # Calculate daily averages for critical features
    df['date'] = df['Formatted Date'].dt.date  # This should now work
    daily_avg = df.groupby('date').agg({
        'Temperature (C)': 'mean',
        'Humidity': 'mean',
        'Wind Speed (km/h)': 'mean',
        'Visibility (km)': 'mean',
        'Pressure (millibars)': 'mean'
    }).reset_index()

    # Extract month from 'Formatted Date'
    df['month'] = df['Formatted Date'].dt.month

    # Calculate monthly mode for 'Precip Type', if there's no mode, set to NA
    monthly_mode = df.groupby('month')['Precip Type'].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else pd.NA).reset_index()
    monthly_mode.columns = ['month', 'Mode']

    # Add wind_strength column categorizing wind speed
    df['wind_strength'] = pd.cut(
        df['Wind Speed (km/h)'] / 3.6, 
        bins=[-1, 1.5, 3.3, 5.4, 7.9, 10.7, 13.8, 17.1, 20.7, 24.4, 28.4, 32.6, float('inf')], 
        labels=[
            'Calm', 'Light Air', 'Light Breeze', 'Gentle Breeze', 'Moderate Breeze', 
            'Fresh Breeze', 'Strong Breeze', 'Near Gale', 'Gale', 'Strong Gale', 
            'Storm', 'Violent Storm'
        ]
    )

    # Calculate monthly averages for critical features
    monthly_avg = df.groupby('month').agg({
        'Temperature (C)': 'mean',
        'Humidity': 'mean',
        'Wind Speed (km/h)': 'mean',
        'Visibility (km)': 'mean',
        'Pressure (millibars)': 'mean'
    }).reset_index()

    # Save daily and monthly transformed data to new CSV files
    daily_avg.to_csv(csv_folder_path + 'daily_avg.csv', index=False)
    monthly_avg.to_csv(csv_folder_path + 'monthly_avg.csv', index=False)

    # Push the file paths to XCom
    kwargs['ti'].xcom_push(key='daily_avg_path', value=csv_folder_path + 'daily_avg.csv')
    kwargs['ti'].xcom_push(key='monthly_avg_path', value=csv_folder_path + 'monthly_avg.csv')

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

def validate_data(**kwargs):
    # Load the transformed data paths from XCom
    daily_path = kwargs['ti'].xcom_pull(key='daily_avg_path')
    monthly_path = kwargs['ti'].xcom_pull(key='monthly_avg_path')

    df_daily = pd.read_csv(daily_path)
    df_monthly = pd.read_csv(monthly_path)

    # Define ranges for validation
    temperature_range = (-50, 50)  # Reasonable temperature range
    humidity_range = (0, 1)  # Humidity as a proportion (0 to 1)
    wind_speed_min = 0  # Wind speed should be >= 0

    validation_issues = []

    # --- Validate Daily Data ---
    # Missing values check
    if df_daily.isnull().any().any():
        validation_issues.append("Daily data contains missing values.")

    # Range checks for daily data
    if not df_daily['Temperature (C)'].between(*temperature_range).all():
        validation_issues.append("Daily Temperature values are out of range.")
    if not df_daily['Humidity'].between(*humidity_range).all():
        validation_issues.append("Daily Humidity values are out of range.")
    if not (df_daily['Wind Speed (km/h)'] >= wind_speed_min).all():
        validation_issues.append("Daily Wind Speed values are out of range.")

    # --- Validate Monthly Data ---
    # Missing values check
    if df_monthly.isnull().any().any():
        validation_issues.append("Monthly data contains missing values.")

    # Range checks for monthly data
    if not df_monthly['Temperature (C)'].between(*temperature_range).all():
        validation_issues.append("Monthly Temperature values are out of range.")
    if not df_monthly['Humidity'].between(*humidity_range).all():
        validation_issues.append("Monthly Humidity values are out of range.")
    if not (df_monthly['Wind Speed (km/h)'] >= wind_speed_min).all():
        validation_issues.append("Monthly Wind Speed values are out of range.")

    # --- Outlier Detection ---
    # Define outlier thresholds for temperature (adjust as needed for context)
    temp_outlier_thresholds = (-30, 40)  # Seasonal reasonable limits
    daily_temp_outliers = df_daily[~df_daily['Temperature (C)'].between(*temp_outlier_thresholds)]
    if not daily_temp_outliers.empty:
        validation_issues.append(f"Daily Temperature outliers detected: {daily_temp_outliers['Temperature (C)'].values}")

    monthly_temp_outliers = df_monthly[~df_monthly['Temperature (C)'].between(*temp_outlier_thresholds)]
    if not monthly_temp_outliers.empty:
        validation_issues.append(f"Monthly Temperature outliers detected: {monthly_temp_outliers['Temperature (C)'].values}")

    # --- Validation Results ---
    if validation_issues:
        for issue in validation_issues:
            print(issue)
        raise ValueError("Validation failed. See issues above.")

    print("Validation passed: All checks succeeded for daily and monthly data.")

# Task definition for validation
validate_task = PythonOperator(
    task_id='validate_task',
    python_callable=validate_data,
    provide_context=True,
    dag=dag,
)

def load_data(**kwargs):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)

    # Retrieve transformed file paths from XCom
    transformed_daily = kwargs['ti'].xcom_pull(key='daily_avg_path')
    transformed_monthly = kwargs['ti'].xcom_pull(key='monthly_avg_path')

    # cursor = conn.cursor()

    # # Create daily_weather table if it doesn't exist
    # ## This step is not needed as the table is created in the next step when the data is loaded
    # cursor.execute('''
    #     CREATE TABLE IF NOT EXISTS daily_weather (
    #         id INTEGER PRIMARY KEY AUTOINCREMENT,
    #         formatted_date TEXT,
    #         temperature_c REAL,
    #         apparent_temperature_c REAL,
    #         humidity REAL,
    #         wind_speed_kmh REAL,
    #         visibility_km REAL,
    #         pressure_millibars REAL,
    #         wind_strength TEXT,
    #         avg_temperature_c REAL,
    #         avg_humidity REAL,
    #         avg_wind_speed_kmh REAL
    #     )
    # ''')

    # # Create monthly_weather table if it doesn't exist
    # cursor.execute('''
    #     CREATE TABLE IF NOT EXISTS monthly_weather (
    #         id INTEGER PRIMARY KEY AUTOINCREMENT,
    #         month INTEGER,
    #         avg_temperature_c REAL,
    #         avg_apparent_temperature_c REAL,
    #         avg_humidity REAL,
    #         avg_visibility_km REAL,
    #         avg_pressure_millibars REAL,
    #         mode_precip_type TEXT
    #     )
    # ''')

    # conn.cursor()


    # Load daily data
    df_daily = pd.read_csv(transformed_daily)
    df_daily.to_sql('daily_weather', conn, if_exists='replace', index=False)

    # Load monthly data
    df_monthly = pd.read_csv(transformed_monthly)
    df_monthly.to_sql('monthly_weather', conn, if_exists='replace', index=False)

    conn.commit()
    conn.close()

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> validate_task >> load_task
