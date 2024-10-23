import mysql.connector
import requests
import json
import csv
from bs4 import BeautifulSoup

db_config = {
    'user': 'root',
    'password': 'password',
    'host': 'localhost',
    'database': 'tikiproductsdb'
}

# Read the list of IDs from the CSV file
ids_to_fetch = []
with open('products-0-200000.csv', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        ids_to_fetch.append(row['id'])

# Add headers to the request
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()

insert_query = """
INSERT INTO products (
    id, name, price, description, url_key, images
) VALUES (
    %(id)s, %(name)s, %(price)s, %(description)s, %(url_key)s, %(images)s
);
"""

try:
    for product_id in ids_to_fetch:
        url = f"https://api.tiki.vn/product-detail/api/v1/products/{product_id}"

        try:
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                product_data = response.json()

                # Normalize the content in the description field
                if product_data.get('description'):
                    soup = BeautifulSoup(product_data['description'], "html.parser")
                    normalized_description = soup.get_text(separator="\n", strip=True)
                else:
                    normalized_description = None

                # Extract information from product_data and prepare for the insert statement
                product_info = {
                    'id': product_data.get('id'),
                    'name': product_data.get('name'),
                    'price': product_data.get('price'),
                    'description': normalized_description,
                    'url_key': product_data.get('url_key'),
                    'images': json.dumps(product_data.get('images')) if product_data.get('images') else None
                }

                # Attempt to insert into database
                try:
                    cursor.execute(insert_query, product_info)
                    connection.commit()
                except mysql.connector.Error as db_error:
                    # Log the specific error and product ID causing the issue
                    print(f"Database error for product ID {product_id}: {db_error}")

            elif response.status_code == 404:
                # Log the 404 error and continue to the next ID
                print(f"Product ID {product_id} not found (404). Skipping...")

            else:
                # For other errors, you can also log them if necessary
                print(f"Failed to fetch data for ID {product_id}: {response.status_code}")

        except Exception as fetch_error:
            # Catch errors related to fetching or processing product data
            print(f"Error processing product ID {product_id}: {fetch_error}")

except Exception as e:
    print(f"An unexpected error occurred: {e}")

finally:
    cursor.close()
    connection.close()
