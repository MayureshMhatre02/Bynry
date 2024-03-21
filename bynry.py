import os
import psycopg2  # Replace with appropriate database library based on your DB
import requests


# Database connection function )
def connect_to_database():
    host = os.getenv("ABC_DB_HOST")
    database = os.getenv("ABC_DB_NAME")
    user = os.getenv("ABC_DB_USER")
    password = os.getenv("ABC_DB_PASSWORD")

    connection = psycopg2.connect(
        host=host, database=database, user=user, password=password
    )
    return connection

# extracting consumer data from ABC Utility's database
def extract_data():
    connection = connect_to_database()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM consumer_table")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


# transform data
def transform_data(consumer_data):
    transformed_data = []
    for row in consumer_data:
        names = row[1].split(" ", 1)  #'FirstName LastName' format
        first_name = names[0]
        last_name = names[1] if len(names) > 1 else ""  # cases with no last name
        address_parts = row[2].split(", ", 1)
        address_line_1 = address_parts[0]
        address_line_2 = address_parts[1] if len(address_parts) > 1 else ""
                
        transformed_data.append(
            (row[0], first_name, last_name, address_line_1, address_line_2, ..., row[9])
        )
    return transformed_data


# loading data into SMART360
def load_data_to_smart360(transformed_data):
    headers = {"Authorization": "Bearer YOUR_SMART360_API_KEY"} 

    for data in transformed_data:
        payload = {
            "ConsumerID": data[0],
            "FirstName": data[1],
            "LastName": data[2],
        }
        response = requests.post(
            "https://YOUR_SMART360_API_ENDPOINT/consumer_data", json=payload, headers=headers
        )
        if response.status_code != 200:
            print(f"Failed to load data for Consumer ID: {data[0]} - {response.text}")
        else:
            print(f"Successfully loaded data for Consumer ID: {data[0]}")

def main():
    consumer_data = extract_data()
    transformed_data = transform_data(consumer_data)
    load_data_to_smart360(transformed_data)

if __name__ == "__main__":
    main()
