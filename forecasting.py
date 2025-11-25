import pandas as pd
import numpy as np
from db_utils import get_engine
from sqlalchemy import text

def calculate_moving_average(series, window=3):
    return series.rolling(window=window).mean().iloc[-1]

def calculate_exponential_smoothing(series, alpha=0.5):
    return series.ewm(alpha=alpha).mean().iloc[-1]

def generate_forecasts(aggregated_data):
    """
    Generates forecasts for each product based on historical monthly sales.
    """
    forecasts = []
    

    products = aggregated_data['product_id'].unique()
    
    for product_id in products:
        product_data = aggregated_data[aggregated_data['product_id'] == product_id].sort_values('month_year')
        

        if len(product_data) < 3:
            continue
            
        sales_series = product_data['quantity']
        

        ma_forecast = calculate_moving_average(sales_series, window=3)
        

        es_forecast = calculate_exponential_smoothing(sales_series, alpha=0.5)
        

        final_forecast = (ma_forecast + es_forecast) / 2
        

        last_month = product_data['month_year'].iloc[-1]

        last_date = pd.to_datetime(last_month + '-01')
        next_date = last_date + pd.DateOffset(months=1)
        next_month_str = next_date.strftime('%Y-%m')

        forecasts.append({
            'product_id': int(product_id),
            'forecast_month': next_month_str,
            'forecast_demand': int(round(final_forecast))
        })
        
    return pd.DataFrame(forecasts)

def save_forecasts(forecast_df):
    engine = get_engine()

    
    forecast_df.to_sql('forecast_results', con=engine, if_exists='append', index=False)
    print(f"Saved {len(forecast_df)} forecast records.")

def run_forecasting(aggregated_data):
    print("Running Demand Forecasting...")
    forecast_df = generate_forecasts(aggregated_data)
    save_forecasts(forecast_df)
    return forecast_df

if __name__ == "__main__":

    pass
