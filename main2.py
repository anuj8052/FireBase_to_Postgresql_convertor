import psycopg2
import json
import logging  # Add logging

logging.basicConfig(filename='log.txt', level=logging.DEBUG)  # Set up logging

# Load Firebase data from JSON file
with open('firebase.json') as f:
    firebase_data = json.load(f)['demo']

# Connect to PostgreSQL
try:
    with psycopg2.connect(
        dbname="my_database",
        user="postgres",
        password="Anujkr@12345",
        host="localhost"
    ) as conn:
        with conn.cursor() as cur:
            # Iterate through Firebase data and insert into PostgreSQL
            for user_id, user_data in firebase_data.items():
                name = user_data.get('name')  # Use .get() to handle missing keys
                email = user_data.get('email')
                age = user_data.get('age')

                if all([name, email, age]):  # Check for missing values
                    cur.execute(
                        "INSERT INTO demo (name, email, age) VALUES (%s, %s, %s)",
                        (name, email, age)
                    )
                    logging.info(f"Inserted user: {name} - {email}")  # Log successful inserts
                else:
                    logging.error(f"Incomplete user data: {user_id}")  # Log errors

except (psycopg2.Error) as e:
    logging.error(f"Database connection error: {e}")
