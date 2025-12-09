# Fintech Project Data Analysis Pipeline

## Project Overview
This project implements a data analysis pipeline for a Fintech dataset. It involves staging raw CSV data into Snowflake, performing initial exploratory data analysis (EDA), and executing advanced transformations to derive deeper insights into customer behavior and transaction trends.

## Workflow Description

The project workflow consists of the following components:

1.  **Data Staging & Ingestion (`main.ipynb`)**:
    -   Connects to Snowflake using credentials from `.env`.
    -   Uploads raw CSV datasets (`customers.csv`, `transactions.csv`, `loans.csv`, `support_tickets.csv`) to a Snowflake stage (`@RAW_DATA`).
    -   Handles file encoding and path resolution for seamless upload.

2.  **Initial Exploratory Data Analysis (`EDA.ipynb`)**:
    -   Fetches data from Snowflake tables (`raw_data_customer_dim`, `raw_data_transaction_fact`, etc.).
    -   Displays basic data structures (head, shape, null values).
    -   Performs initial visualization, such as the distribution of transaction types.

3.  **Advanced Analysis (`advanced_eda.py`)**:
    -   **Data Integration**: Merges Transaction and Customer datasets to link financial activities with demographic profiles.
    -   **Feature Engineering**:
        -   Parses dates for time-series analysis.
        -   Calculates aggregated metrics like 'Average Transaction Value' per customer.
    -   **Advanced Visualization**:
        -   Transaction Volume by Country.
        -   Daily/Monthly Transaction Trends.
        -   Customer Segmentation based on spending behavior.

## Setup Instructions

### Prerequisites
-   Python 3.10+
-   Snowflake Account
-   Power BI

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

## Data Schema

The analysis relies on the following key tables:

-   **`raw_data_customer_dim`**: Contains demographic info (ID, Name, Age, Country, Signup Date).
-   **`raw_data_transaction_fact`**: Records financial transactions (ID, Amount, Type, Date).
-   **`raw_data_loans_dim`**: Details loan applications and status.
-   **`raw_data_support_ticket_dim`**: Logs customer support interactions.

## How to Run

1.  **Run the ETL Process**:
    Open and run `main.ipynb` to upload fresh data to Snowflake.

2.  **Run Initial EDA**:
    Open and run `EDA.ipynb` for a quick look at the data quality and basic stats.

3.  **Run Advanced Analysis**:
    Execute the Python script to generate advanced insights:
    ```bash
    python advanced_eda.py
    ```
