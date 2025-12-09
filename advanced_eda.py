
import os
import snowflake.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
import datetime

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
    query_transactions = "SELECT * FROM raw_data_transaction_fact"
    query_customers = "SELECT * FROM raw_data_customer_dim"
    
    df_trans = fetch_data(query_transactions)
    df_cust = fetch_data(query_customers)

    print(f"Transactions shape: {df_trans.shape}")
    print(f"Customers shape: {df_cust.shape}")

    # 2. Data Cleaning & Type Conversion
    df_trans['TRANSACTION_DATE'] = pd.to_datetime(df_trans['TRANSACTION_DATE'])
    # Convert 'AMOUNT' to numeric, coercing errors just in case
    df_trans['AMOUNT'] = pd.to_numeric(df_trans['AMOUNT'], errors='coerce').fillna(0)
    
    df_cust['SIGNUP_DATE'] = pd.to_datetime(df_cust['SIGNUP_DATE'])

    # 3. Merging Data
    df_merged = pd.merge(df_trans, df_cust, on='CUSTOMER_ID', how='inner')
    print(f"Merged dataset shape: {df_merged.shape}")
    
    # 4. Advanced EDA Transformations
    
    # --- EXISTING ANALYSIS ---
    
    # Insight 1: Transaction Volume by Country
    print("Generating Country Volume Chart...")
    country_volume = df_merged.groupby('COUNTRY')['AMOUNT'].sum().sort_values(ascending=False).head(10)
    
    plt.figure(figsize=(10, 6))
    sns.barplot(x=country_volume.values, y=country_volume.index, palette='viridis')
    plt.title('Top 10 Countries by Transaction Volume')
    plt.xlabel('Total Transaction Amount')
    plt.ylabel('Country')
    plt.tight_layout()
    plt.savefig('eda_country_volume.png')
    plt.close() # Close plot to free memory
    
    # Insight 2: Average Transaction Value per Customer
    print("Generating Customer Avg Spend Chart...")
    customer_avg_spend = df_merged.groupby('CUSTOMER_ID')['AMOUNT'].mean()
    
    plt.figure(figsize=(10, 6))
    sns.histplot(customer_avg_spend, bins=30, kde=True, color='blue')
    plt.title('Distribution of Average Transaction Value per Customer')
    plt.xlabel('Average Transaction Amount')
    plt.ylabel('Frequency')
    plt.tight_layout()
    plt.savefig('eda_customer_avg_spend.png')
    plt.close()

    # Insight 3: Monthly Transaction Trends
    print("Generating Monthly Trend Chart...")
    # Create a copy to avoid SettingWithCopy warnings on the original df if needed, though mostly safe here
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
    plt.close()

    # --- NEW ANALYSIS ---

    # Insight 4: RFM Analysis (Recency, Frequency, Monetary)
    print("Performing RFM Analysis...")
    latest_date = df_merged['TRANSACTION_DATE'].max()
    
    # Calculate RFM metrics
    rfm = df_merged.groupby('CUSTOMER_ID').agg({
        'TRANSACTION_DATE': lambda x: (latest_date - x.max()).days, # Recency
        'TRANSACTION_ID': 'count',                                   # Frequency
        'AMOUNT': 'sum'                                              # Monetary
    }).rename(columns={
        'TRANSACTION_DATE': 'Recency',
        'TRANSACTION_ID': 'Frequency',
        'AMOUNT': 'Monetary'
    })
    
    # Simple Scoring (Quantiles) - handle potential duplicates/small data sizes gracefully
    try:
        rfm['R_Score'] = pd.qcut(rfm['Recency'], 5, labels=[5, 4, 3, 2, 1]) # Higher score for lower recency
        rfm['F_Score'] = pd.qcut(rfm['Frequency'].rank(method='first'), 5, labels=[1, 2, 3, 4, 5])
        rfm['M_Score'] = pd.qcut(rfm['Monetary'], 5, labels=[1, 2, 3, 4, 5])
    except ValueError as e:
        print(f"Warning: Not enough unique values for 5 bins in RFM. Using rank-based or simplified approach. {e}")
        # Fallback if dataset is too small for qcut
        rfm['R_Score'] = pd.cut(rfm['Recency'], 5, labels=[5, 4, 3, 2, 1])
        rfm['F_Score'] = pd.cut(rfm['Frequency'], 5, labels=[1, 2, 3, 4, 5])
        rfm['M_Score'] = pd.cut(rfm['Monetary'], 5, labels=[1, 2, 3, 4, 5])
        
    # Combine scores (simple concatenation or summing, here we create a Segment map)
    # We'll visualize Recency vs Monetary to identify 'Champions' vs 'At Risk'
    
    plt.figure(figsize=(10, 8))
    sns.scatterplot(data=rfm, x='Recency', y='Monetary', hue='Frequency', size='Frequency', sizes=(20, 200), palette='coolwarm', alpha=0.7)
    plt.title('RFM Analysis: Recency vs Monetary Value')
    plt.xlabel('Recency (Days since last txn)')
    plt.ylabel('Total Monetary Value')
    plt.legend(title='Frequency', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig('eda_rfm_segments.png')
    plt.close()

    # Insight 5: Churn Risk Analysis
    print("Analyzing Churn Risk...")
    # Definition: Customer is 'Churned' if inactive for > 90 days
    churn_threshold = 90
    rfm['Status'] = rfm['Recency'].apply(lambda x: 'Churned (>90 days)' if x > churn_threshold else 'Active')
    
    churn_counts = rfm['Status'].value_counts()
    
    plt.figure(figsize=(8, 8))
    plt.pie(churn_counts, labels=churn_counts.index, autopct='%1.1f%%', colors=['#ff9999','#66b3ff'], startangle=90)
    plt.title('Customer Churn Risk Distribution')
    plt.savefig('eda_churn_risk.png')
    plt.close()

    # Insight 6: Transaction Type Trends over Time
    print("Analyzing Transaction Type Trends...")
    # Pivot table: Month_Year index, Type columns, count/sum values
    # Group by string representation of period to avoid plotting issues
    df_merged['Month_Str'] = df_merged['Month_Year'].astype(str)
    type_trend = df_merged.pivot_table(index='Month_Str', columns='TRANSACTION_TYPE', values='TRANSACTION_ID', aggfunc='count').fillna(0)
    
    # Sort index chronologically (it should be sorted if Month_Year was proper, but string sort works for YYYY-MM)
    type_trend = type_trend.sort_index()

    plt.figure(figsize=(12, 6))
    type_trend.plot(kind='area', stacked=True, alpha=0.8, colormap='tab10', figsize=(12,6))
    plt.title('Transaction Type Evolution Over Time')
    plt.xlabel('Month')
    plt.ylabel('Number of Transactions')
    plt.legend(title='Transaction Type')
    plt.tight_layout()
    plt.savefig('eda_type_trends.png')
    plt.close()

    print("\nAdvanced EDA completed successfully.")
    print("Generated: eda_country_volume.png, eda_customer_avg_spend.png, eda_monthly_trend.png")
    print("Generated: eda_rfm_segments.png, eda_churn_risk.png, eda_type_trends.png")

if __name__ == "__main__":
    main()
