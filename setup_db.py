import mysql.connector
import os

def setup_database():

    db_user = os.getenv('DB_USER', 'root')
    db_password = os.getenv('DB_PASSWORD', 'root')
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'retail_db')

    try:

        print("Connecting to MySQL server...")
        conn = mysql.connector.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        cursor = conn.cursor()


        print(f"Creating database '{db_name}' if it doesn't exist...")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        conn.commit()
        

        conn.database = db_name
        

        print("Reading schema.sql...")
        with open('schema.sql', 'r') as f:
            schema_sql = f.read()


        commands = schema_sql.split(';')
        
        print("Executing schema commands...")
        for command in commands:
            if command.strip():
                try:
                    cursor.execute(command)
                except mysql.connector.Error as err:
                    print(f"Error executing command: {err}")
                    print(f"Command: {command}")

        conn.commit()
        print("Database setup completed successfully.")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed.")

if __name__ == "__main__":
    setup_database()
