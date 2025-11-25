import pandas as pd
from db_utils import get_engine

def fetch_sales_data():
    """Fetches sales data joined with products."""
    engine = get_engine()
    query = """
        SELECT 
            s.sale_date,
            s.product_id,
            p.name as product_name,
            p.category,
            s.quantity,
            s.total_amount
        FROM sales s
        JOIN products p ON s.product_id = p.product_id
    """
    df = pd.read_sql(query, engine)
    df['sale_date'] = pd.to_datetime(df['sale_date'])
    return df

def aggregate_monthly_sales(df):
    """Aggregates sales by month and product."""

    df['month_year'] = df['sale_date'].dt.to_period('M')
    
    monthly_sales = df.groupby(['product_id', 'product_name', 'category', 'month_year']).agg({
        'quantity': 'sum',
        'total_amount': 'sum'
    }).reset_index()
    

    monthly_sales['month_year'] = monthly_sales['month_year'].astype(str)
    
    return monthly_sales

def run_etl():
    print("Running ETL Pipeline...")
    raw_data = fetch_sales_data()
    print(f"Fetched {len(raw_data)} sales records.")
    
    aggregated_data = aggregate_monthly_sales(raw_data)
    print(f"Aggregated into {len(aggregated_data)} monthly records.")
    
    return aggregated_data

if __name__ == "__main__":
    run_etl()
