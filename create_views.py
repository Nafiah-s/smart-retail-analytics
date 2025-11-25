from db_utils import get_engine
from sqlalchemy import text

def create_views():
    engine = get_engine()
    
    views = [
        """
        CREATE OR REPLACE VIEW daily_sales_view AS
        SELECT 
            s.sale_date,
            SUM(s.total_amount) as daily_revenue,
            COUNT(s.sale_id) as transaction_count,
            SUM(s.quantity) as items_sold
        FROM sales s
        GROUP BY s.sale_date;
        """,
        """
        CREATE OR REPLACE VIEW product_performance_view AS
        SELECT 
            p.product_id,
            p.name as product_name,
            c.category_name,
            SUM(s.quantity) as total_units_sold,
            SUM(s.total_amount) as total_revenue,
            AVG(s.quantity) as avg_units_per_sale
        FROM products p
        JOIN sales s ON p.product_id = s.product_id
        -- Assuming category is a string in products table for now, or join if normalized
        -- Based on seed_data, category is a column in products
        JOIN (SELECT DISTINCT category as category_name FROM products) c ON p.category = c.category_name
        GROUP BY p.product_id, p.name, c.category_name;
        """,
        """
        CREATE OR REPLACE VIEW customer_insights_view AS
        SELECT 
            c.customer_id,
            c.full_name,
            c.city,
            COUNT(s.sale_id) as total_purchases,
            SUM(s.total_amount) as lifetime_value,
            MAX(s.sale_date) as last_purchase_date
        FROM customers c
        JOIN sales s ON c.customer_id = s.customer_id
        GROUP BY c.customer_id, c.full_name, c.city;
        """
    ]
    

    
    views[1] = """
        CREATE OR REPLACE VIEW product_performance_view AS
        SELECT 
            p.product_id,
            p.name as product_name,
            p.category,
            SUM(s.quantity) as total_units_sold,
            SUM(s.total_amount) as total_revenue
        FROM products p
        JOIN sales s ON p.product_id = s.product_id
        GROUP BY p.product_id, p.name, p.category;
    """

    print("Creating database views...")
    with engine.connect() as conn:
        for sql in views:
            try:
                conn.execute(text(sql))
                print("View created successfully.")
            except Exception as e:
                print(f"Error creating view: {e}")
                
    print("All views created.")

if __name__ == "__main__":
    create_views()
