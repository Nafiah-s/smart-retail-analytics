import etl_pipeline
import forecasting
import inventory_analytics
import sys

def main():
    print("Starting Smart Retail Analytics System...")
    
    try:

        aggregated_data = etl_pipeline.run_etl()
        
        if aggregated_data.empty:
            print("No data found. Please run seed_data.py first.")
            return


        forecast_df = forecasting.run_forecasting(aggregated_data)
        

        inventory_analytics.run_inventory_analytics(forecast_df)
        
        print("Workflow completed successfully.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
