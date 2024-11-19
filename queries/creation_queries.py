from db_connection import create_connection

tables = {
    "Customers": """
        CREATE TABLE Customers (
            CustomerId INT PRIMARY KEY,
            Name VARCHAR(100) NOT NULL,
            Email VARCHAR(255) NOT NULL,
            Age INT CHECK (Age >= 0),
            CONSTRAINT UQ_Customer_Email UNIQUE (Email)
        );
    """.strip(),
    "Orders": """
        CREATE TABLE Orders (
            OrderId INT PRIMARY KEY,
            CustomerId INT NOT NULL,
            Total DECIMAL(10,2) NOT NULL CHECK (Total >= 0),
            CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerId)
                REFERENCES Customers (CustomerId)
        );
    """.strip(),
    "OrderLines": """
        CREATE TABLE OrderLines (
            OrderLineId INT PRIMARY KEY,
            OrderId INT NOT NULL,
            Qty INT NOT NULL CHECK (Qty > 0),
            Price DECIMAL(10,2) NOT NULL CHECK (Price >= 0),
            LineTotal AS (Price * Qty) PERSISTED,
            ProductId INT NOT NULL,
            CONSTRAINT FK_OrderLines_Orders FOREIGN KEY (OrderId)
                REFERENCES Orders (OrderId)
        );
    """.strip()
}

def query(str_query):
    with create_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(str_query)
            if str_query.strip().upper().startswith('SELECT'):
                return cursor.fetchall()
            else:
                conn.commit()
                return cursor.rowcount

def drop_tables():
    for key in reversed(tables):
        query(f"DROP TABLE IF EXISTS dbo.{key}")
        print(f"[{key}] SUCCESSFULLY DROPPED")

def create_tables():
    for key, value in tables.items():
        query(value)
        print(f"Created table: {key}")

def reset_tables():
    drop_tables()
    create_tables()