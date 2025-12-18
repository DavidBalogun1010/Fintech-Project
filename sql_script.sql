- Creating the LayerOuts ---

CREATE WAREHOUSE FintechWH;
CREATE DATABASE FintechDB;
CREATE SCHEMA FintechSchema;

-- Creatimg the staging layer ---

CREATE STAGE raw_data;
CREATE STAGE transform_data;
CREATE STAGE business_ready_data;



---- Creating the tables for the Raw staging Layer --- 
CREATE OR REPLACE TABLE raw_data_customer_dim (
    Customer_id VARCHAR(40),
    Name VARCHAR(40),
    Age INT,
    Signup_date DATE,
    Country VARCHAR(40)
);

CREATE OR REPLACE TABLE raw_data_Loans_dim (
    loan_id VARCHAR(200),
    Customer_id VARCHAR(30),
    Loan_amount INT,
    loan_status VARCHAR(30),
    loan_date DATE
);

CREATE OR REPLACE TABLE raw_data_support_ticket_dim (
    ticker_id VARCHAR(40),
    customer_id VARCHAR(40),
    issue_type VARCHAR(40),
    Severity VARCHAR(30),
    Created_at DATE
);

DROP TABLE raw_data_supported_ticket_dim;

/* # Creating the Transaction fact table */
CREATE OR REPLACE TABLE raw_data_transaction_fact (
    transaction_id VARCHAR(40),
    Customer_id VARCHAR(40),
    Amount INT,
    Transaction_type VARCHAR(40),
    Transaction_date DATE
);

LIST @RAW_DATA;


--  Creating a file format ---
CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = CSV
  SKIP_HEADER = 1;



-- Copying into the table from the raw staging layer ---
COPY INTO raw_data_customer_dim
FROM @raw_data/customers.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    SKIP_HEADER = 1,
    FIELD_DELIMITER = ',',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
)
ON_ERROR = 'CONTINUE';


-- copy Loans dim 
COPY INTO raw_data_Loans_dim
FROM @raw_data/loans.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    SKIP_HEADER = 1,
    FIELD_DELIMITER = ',',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
)
ON_ERROR = 'CONTINUE';



-- copy into support ticket dim 
COPY INTO raw_data_support_ticket_dim
FROM @raw_data/support_tickets.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    SKIP_HEADER = 1,
    FIELD_DELIMITER = ',',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
)
ON_ERROR = 'CONTINUE';


-- copy into transaction 
COPY INTO raw_data_transaction_fact
FROM @raw_data/transactions.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    SKIP_HEADER = 1,
    FIELD_DELIMITER = ',',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
)
ON_ERROR = 'CONTINUE';

SELECT * FROM raw_data_customer_dim;
SELECT * FROM raw_data_loans_dim;
SELECT * FROM raw_data_support_ticket_dim;
SELECT * FROM raw_data_transaction_fact;






-- Saving the table into the transformed layer --- 

COPY INTO @transform_data/customers
FROM (
    SELECT * FROM raw_data_customer_dim
)
FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = ',', COMPRESSION = GZIP)
HEADER = TRUE;


list @transform_data;

Remove @transform_data;

-- copy Loan data

COPY INTO @transform_data/Loans/
FROM (
    SELECT * FROM raw_data_loans_dim
)
FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = ',', COMPRESSION = GZIP)
HEADER = TRUE;


-- Copy support ticket data 
COPY INTO @transform_data/support_ticket/
FROM (
    SELECT * FROM raw_data_support_ticket_dim
)
FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = ',', COMPRESSION = GZIP)
HEADER = TRUE;


--copy transaction data 
COPY INTO @transform_data/transaction/
FROM (
    SELECT * FROM raw_data_transaction_fact
)
FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = ',', COMPRESSION = GZIP)
HEADER = TRUE;

