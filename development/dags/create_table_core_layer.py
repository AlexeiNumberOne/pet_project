from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

default_args = {
    'owner':'airflow',
    'start_date': datetime(2026,1,1)
}


def create_table_core_layer():
    engine = create_engine('postgresql://postgres:postgres@postgres_dwh:5432/postgres')
    Session = sessionmaker(bind=engine)
    session = Session()

    sql = text(f"""
                BEGIN;

                CREATE TABLE tickers (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10),
                    company_name VARCHAR(100),
                    sector VARCHAR(50),
                    industry VARCHAR(100),
                    country VARCHAR(2) DEFAULT 'US',
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE minute_bars(
                    ticker_id INTEGER NOT NULL,
                    time_stamp TIMESTAMPTZ NOT NULL,
                    open_price DECIMAL(10,2),
                    high_price DECIMAL(10,2),
                    low_price DECIMAL(10,2),
                    close_price DECIMAL(10,2),
                    volume INT,
                    vwap DECIMAL(10,4),
                    transactions INT,

                    PRIMARY KEY (ticker_id, time_stamp),
                    FOREIGN KEY (ticker_id) REFERENCES tickers(id)
                );

                CREATE INDEX idx_minute_bars_timestamp ON minute_bars(time_stamp);
                CREATE INDEX idx_minute_bars_ticker_ts_desc ON minute_bars(ticker_id, time_stamp DESC);

                INSERT INTO tickers (ticker, company_name, sector, industry) VALUES 
                ('AAPL', 'Apple Inc', 'Technology', 'Consumer Electronics'),
                ('MSFT', 'Microsoft Corporation', 'Technology', 'Software - Infrastructure'),
                ('GOOGL', 'Alphabet Inc (Class A)', 'Communication Services', 'Internet Content & Information'),
                ('GOOG', 'Alphabet Inc (Class C)', 'Communication Services', 'Internet Content & Information'),
                ('AMZN', 'Amazon.com Inc', 'Consumer Cyclical', 'Internet Retail'),
                ('META', 'Meta Platforms Inc', 'Communication Services', 'Internet Content & Information'),
                ('TSLA', 'Tesla Inc', 'Consumer Cyclical', 'Auto Manufacturers'),
                ('NVDA', 'NVIDIA Corporation', 'Technology', 'Semiconductors'),
                ('INTC', 'Intel Corporation', 'Technology', 'Semiconductors'),
                ('AMD', 'Advanced Micro Devices Inc', 'Technology', 'Semiconductors'),
                ('ADBE', 'Adobe Inc', 'Technology', 'Software - Infrastructure'),
                ('JPM', 'JPMorgan Chase & Co', 'Financial Services', 'Banks - Diversified'),
                ('BAC', 'Bank of America Corporation', 'Financial Services', 'Banks - Diversified'),
                ('GS', 'The Goldman Sachs Group Inc', 'Financial Services', 'Capital Markets'),
                ('MS', 'Morgan Stanley', 'Financial Services', 'Capital Markets'),
                ('V', 'Visa Inc', 'Financial Services', 'Credit Services'),
                ('MA', 'Mastercard Incorporated', 'Financial Services', 'Credit Services'),
                ('PG', 'The Procter & Gamble Company', 'Consumer Defensive', 'Household & Personal Products'),
                ('KO', 'The Coca-Cola Company', 'Consumer Defensive', 'Beverages - Non-Alcoholic'),
                ('PEP', 'PepsiCo Inc', 'Consumer Defensive', 'Beverages - Non-Alcoholic'),
                ('WMT', 'Walmart Inc', 'Consumer Defensive', 'Discount Stores'),
                ('MCD', 'McDonald''s Corporation', 'Consumer Cyclical', 'Restaurants'),
                ('BABA', 'Alibaba Group Holding Limited', 'Consumer Cyclical', 'Internet Retail'),
                ('TSM', 'Taiwan Semiconductor Manufacturing Company Limited', 'Technology', 'Semiconductors'),
                ('ASML', 'ASML Holding NV', 'Technology', 'Semiconductor Equipment & Materials'),
                ('JNJ', 'Johnson & Johnson', 'Healthcare', 'Drug Manufacturers - General'),
                ('PFE', 'Pfizer Inc', 'Healthcare', 'Drug Manufacturers - General'),
                ('MRK', 'Merck & Co Inc', 'Healthcare', 'Drug Manufacturers - General'),
                ('UNH', 'UnitedHealth Group Incorporated', 'Healthcare', 'Healthcare Plans'),
                ('ABBV', 'AbbVie Inc', 'Healthcare', 'Drug Manufacturers - General');

                COMMIT;
                """)

    session.execute(sql)
    session.commit()

with DAG(
    dag_id = 'create_table_core_layer',
    default_args= default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    create_table_core_layer = PythonOperator(
        task_id='create_table_core_layer',
        python_callable=create_table_core_layer,
    )

    create_table_core_layer