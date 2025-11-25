import pandas as pd
from db_utils import get_engine

def check_inventory_levels(forecast_df):
    engine = get_engine()
    

    inventory_df = pd.read_sql("SELECT * FROM inventory", engine)
    

    merged_df = pd.merge(inventory_df, forecast_df, on='product_id', how='left')
    
    alerts = []
    
    for _, row in merged_df.iterrows():
        product_id = row['product_id']
        current_stock = row['stock_available']
        forecast_demand = row['forecast_demand']
        
        if pd.isna(forecast_demand):
            continue
            

        if current_stock < forecast_demand:
            shortage = forecast_demand - current_stock
            alerts.append({
                'product_id': product_id,
                'alert_type': 'Low Stock',
                'message': f"Stock ({current_stock}) is below forecasted demand ({forecast_demand}). Restock {shortage} units."
            })
        elif current_stock > forecast_demand * 3:
             alerts.append({
                'product_id': product_id,
                'alert_type': 'Overstock',
                'message': f"Stock ({current_stock}) is significantly higher than demand ({forecast_demand})."
            })
            
    return pd.DataFrame(alerts)

def run_inventory_analytics(forecast_df):
    print("Running Inventory Analytics...")
    alerts_df = check_inventory_levels(forecast_df)
    
    if not alerts_df.empty:
        print("\n--- INVENTORY ALERTS ---")
        print(alerts_df[['product_id', 'alert_type', 'message']].to_string(index=False))
        print("------------------------\n")
    else:
        print("No immediate inventory alerts.")
        
    return alerts_df
