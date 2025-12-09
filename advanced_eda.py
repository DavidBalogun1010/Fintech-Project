
import os
import snowflake.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_snowflake_connection():
    """Establishes a connection to Snowflake."""
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'FintechWH'),
            database=os.getenv('SNOWFLAKE_DATABASE', 'FintechDB'),
            schema=os.getenv('SNOWFLAKE_SCHEMA', 'FintechSchema')
        )
        print("Successfully connected to Snowflake")
        return conn
    except Exception as e:
        print(f"Failed to connect to Snowflake: {str(e)}")
        raise

def fetch_data(query):
    """Fetches data from Snowflake executing the given query."""
    conn = get_snowflake_connection()
    try:
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()

def main():
    print("Fetching data from Snowflake...")
    
    # 1. Fetch relevant data
    # Limiting to a reasonable number if datasets are huge, but here we select all for analysis
    query_transactions = "SELECT * FROM raw_data_transaction_fact"
    query_customers = "SELECT * FROM raw_data_customer_dim"
    
    df_trans = fetch_data(query_transactions)
    df_cust = fetch_data(query_customers)

    print(f"Transactions shape: {df_trans.shape}")
    print(f"Customers shape: {df_cust.shape}")

    # 2. Data Cleaning & Type Conversion
    # Ensure date columns are datetime
    df_trans['TRANSACTION_DATE'] = pd.to_datetime(df_trans['TRANSACTION_DATE'])
    df_cust['SIGNUP_DATE'] = pd.to_datetime(df_cust['SIGNUP_DATE'])

    # 3. Merging Data
    # Join Transactions with Customer data to analyze behavior by demographic
    df_merged = pd.merge(df_trans, df_cust, on='CUSTOMER_ID', how='inner')
    print(f"Merged dataset shape: {df_merged.shape}")
    
    # 4. Advanced EDA Transformations
    
    # Insight 1: Transaction Volume by Country
    # This helps identify which markets are most active
    country_volume = df_merged.groupby('COUNTRY')['AMOUNT'].sum().sort_values(ascending=False).head(10)
    
    plt.figure(figsize=(10, 6))
    sns.barplot(x=country_volume.values, y=country_volume.index, palette='viridis')
    plt.title('Top 10 Countries by Transaction Volume')
    plt.xlabel('Total Transaction Amount')
    plt.ylabel('Country')
    plt.tight_layout()
    plt.savefig('eda_country_volume.png')
    print("Saved 'eda_country_volume.png'")
    
    # Insight 2: Average Transaction Value per Customer (Customer Segmentation)
    # Identify high-value customers
    customer_avg_spend = df_merged.groupby('CUSTOMER_ID')['AMOUNT'].mean()
    
    plt.figure(figsize=(10, 6))
    sns.histplot(customer_avg_spend, bins=30, kde=True, color='blue')
    plt.title('Distribution of Average Transaction Value per Customer')
    plt.xlabel('Average Transaction Amount')
    plt.ylabel('Frequency')
    plt.tight_layout()
    plt.savefig('eda_customer_avg_spend.png')
    print("Saved 'eda_customer_avg_spend.png'")
    
    # Insight 3: Monthly Transaction Trends
    # understand seasonality or growth
    df_merged['Month_Year'] = df_merged['TRANSACTION_DATE'].dt.to_period('M')
    monthly_trend = df_merged.groupby('Month_Year')['AMOUNT'].sum()
    
    plt.figure(figsize=(12, 6))
    monthly_trend.plot(kind='line', marker='o', color='green')
    plt.title('Monthly Transaction Volume Trend')
    plt.xlabel('Month')
    plt.ylabel('Total Transaction Amount')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig('eda_monthly_trend.png')
    print("Saved 'eda_monthly_trend.png'")

    print("\nAdvanced EDA completed successfully. Check the generated PNG files for visualizations.")

if __name__ == "__main__":
    main()
