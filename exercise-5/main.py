import psycopg2
import csv

def create_tables(cursor):
    commands = (
        """
        CREATE TABLE accounts (
            customer_id INT PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            address_1 TEXT NOT NULL,
            address_2 TEXT,
            city TEXT NOT NULL,
            state TEXT NOT NULL,
            zip_code TEXT NOT NULL,
            join_date TIMESTAMP NOT NULL
        );

        """,
        """
        CREATE TABLE products (
            product_id INT PRIMARY KEY,
            product_code INT,
            product_description TEXT
        );
        CREATE INDEX inx_products_product_code ON products(product_code);
        """,
        """
        CREATE TABLE transactions (
            transaction_id TEXT PRIMARY KEY,
            transaction_date TIMESTAMP NOT NULL,
            product_id INT REFERENCES products(product_id),
            quantity INT,
            account_id INT REFERENCES accounts(customer_id)
        
        
        );
        """
        )
    try:
        for command in commands:
            cursor.execute(command)
    except (Exception, psycopg2.DatabaseError) as e:
        print(e)
    

    
def insert_data(cursor):
    with open("data/accounts.csv", "r") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
           
            cursor.execute(
               "INSERT INTO accounts VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
               ,(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8])
            )
    with open("data/products.csv", "r") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
           
            cursor.execute(
               "INSERT INTO products VALUES (%s, %s, %s);"
               ,(row[0],row[1],row[2])
            )

    with open("data/transactions.csv", "r") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
           
            cursor.execute(
               "INSERT INTO transactions VALUES (%s, %s, %s, %s, %s);"
               ,(row[0],row[1],row[2],row[3],row[4])
            )

    



def main():
    host = "localhost"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    cur = conn.cursor()

    create_tables(cur)
    insert_data(cur)

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
