# Fintech Data Unification & Analysis Pipeline

## Project Mission: Solving Data Silos
In the rapidly evolving fintech landscape, data is often fragmented across disparate systems—Customer Relationship Management (CRM) tools, Transaction Processing Systems, Loan Management Software, and Support Ticketing Data. These **Data Silos** prevent a holistic view of the business and hinder effective decision-making.

**The Goal**: This project addresses this challenge by consolidating these isolated data streams into a **Unified Source of Truth**. By staging, cleaning, and integrating raw data into a centralized Snowflake Data Warehouse, we enable consistent, powerful downstream analysis for advanced insights, reporting, and machine learning.

## Solution Architecture

The pipeline transforms raw, siloed CSV data into actionable intelligence:

1.  **Raw Data Ingestion (The Silos)**:
    -   `customers.csv`: Profile and demographic data.
    -   `transactions.csv`: Financial transaction logs.
    -   `loans.csv`: Loan application and status records.
    -   `support_tickets.csv`: Customer service interaction logs.

2.  **Staging & Integration (The Bridge)**:
    -   **ETL Process (`main.ipynb`)**: A robust Python script that validates, cleans, and uploads raw data to a Snowflake Stage (`@RAW_DATA`). It acts as the bridge, moving data from local silos to the cloud.

3.  **Unified Source of Truth (Snowflake)**:
    -   Data is structured into facts and dimensions (`customer_dim`, `transaction_fact`, etc.), providing a single, trusted schema for the entire organization.

4.  **Downstream Analysis (The Value)**:
    -   **Exploratory Data Analysis (`EDA.ipynb`)**: Initial quality checks and distribution analysis.
    -   **Advanced Insights (`advanced_eda.py`)**: 
        -   **RFM Segmentation**: Classifies customers based on Recency, Frequency, and Monetary value.
        -   **Churn Risk Prediction**: Identifies customers inactive for >90 days to proactively prevent attrition.
        -   **Trend Analysis**: Tracks the evolution of transaction types (POS, Bill Payment, Transfer) over time.
    -   **BI Dashboards**: Power BI (or similar tools) can connect directly to this unified layer for real-time reporting.


### Key Metrics & Analysis

The dashboard includes executive KPIs such as:

- Total transaction value (YTD)

- Month-over-Month and Year-over-Year growth

- Active customer counts

- Loan portfolio value

- Support ticket volume (MTD)

Advanced measures were implemented to normalise performance per customer, assess loan exposure using 


## Setup Instructions

### Prerequisites
-   Python 3.10+
-   Snowflake Account
-   Power BI (Optional for Dashboards)

### Installation
1.  **Clone the repository**:
    ```bash
    git clone <repository_url>
    cd Fintech_Project
    ```

2.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Environment Configuration**:
    Create a `.env` file in the root directory with your Snowflake credentials:
    ```env
    SNOWFLAKE_USER=<your_user>
    SNOWFLAKE_PASSWORD=<your_password>
    SNOWFLAKE_ACCOUNT=<your_account>
    SNOWFLAKE_WAREHOUSE=FintechWH
    SNOWFLAKE_DATABASE=FintechDB
    SNOWFLAKE_SCHEMA=FintechSchema
    ```

## Data Schema & Unified Model

The analysis relies on a star-schema inspired design:

-   **`raw_data_customer_dim`**: The central profile for every user.
-   **`raw_data_transaction_fact`**: Linked to customers; the heartbeat of financial activity.
-   **`raw_data_loans_dim`**: Linked to customers; provides risk and credit context.
-   **`raw_data_support_ticket_dim`**: Linked to customers; provides satisfaction and engagement context.

## How to Run

1.  **Ingest Data (Break the Silos)**:
    Run `main.ipynb` to upload fresh data from your local environment to the Snowflake Unified Source.

2.  **Verify Data Quality**:
    Run `EDA.ipynb` to ensure the ingestion was successful and the data is clean.

3.  **Generate Insights**:
    Execute the advanced analysis script to derive business value:
    ```bash
    python advanced_eda.py
    ```
