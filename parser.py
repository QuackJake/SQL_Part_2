from lxml import etree
from decimal import Decimal
from queries.queries import query
from db_connection import create_connection

def sanitize_value(value):
    return value.replace("'", "''") if value else ""

def insert_customer_if_not_exists(customer_data: dict, cursor) -> int:
    customer_id = customer_data['customer_id']
    name = sanitize_value(customer_data['name'])
    email = sanitize_value(customer_data['email'])
    age = customer_data['age']

    check_query = f"""
        SELECT CustomerId
        FROM Customers
        WHERE Email = '{email}'
    """

    cursor.execute(check_query)
    result = cursor.fetchone()

    if result:
        existing_customer_id = result[0]
        print(f"Customer found with ID {existing_customer_id}")
        return existing_customer_id

    insert_query = f"""
        INSERT INTO Customers (CustomerId, Name, Email, Age)
        VALUES ({customer_id}, 
                '{name}', 
                '{email}', 
                {age})
    """
    cursor.execute(insert_query)
    return customer_id

def insert_order(order_data: list, cursor) -> None:
    insert_query = f"""
        INSERT INTO Orders (OrderId, CustomerId, Total)
        VALUES (?, ?, ?)
    """
    cursor.executemany(insert_query, order_data)

def insert_order_line(line_data: list, cursor) -> None:
    insert_query = f"""
        INSERT INTO OrderLines (OrderLineId, OrderId, Qty, Price, ProductId)
        VALUES (?, ?, ?, ?, ?)
    """
    cursor.executemany(insert_query, line_data)

def parse_and_load_xml(xml_file_path: str, num_chunks: int = 1) -> None:
    try:
        with open(xml_file_path, 'rb') as xml_file:
            context = etree.iterparse(xml_file, events=('end',), tag='{http://schemas.datacontract.org/2004/07/DataGenerator}Customer')

            ns = {'ns': 'http://schemas.datacontract.org/2004/07/DataGenerator'}

            customer_data_batch = []
            order_data_batch = []
            order_line_data_batch = []

            with create_connection() as conn:
                cursor = conn.cursor()

                chunk_size = 0
                for event, customer in context:
                    customer_data = {
                        'customer_id': int(customer.find('ns:CustomerId', namespaces=ns).text),
                        'name': customer.find('ns:Name', namespaces=ns).text.replace("'", "''"),
                        'email': customer.find('ns:Email', namespaces=ns).text.replace("'", "''"),
                        'age': int(customer.find('ns:Age', namespaces=ns).text)
                    }

                    customer_id = insert_customer_if_not_exists(customer_data, cursor)

                    orders = customer.find('ns:Orders', namespaces=ns)
                    if orders is not None:
                        for order in orders.findall('ns:Order', namespaces=ns):
                            order_data = (
                                int(order.find('ns:OrderId', namespaces=ns).text),
                                customer_id,  # Use the existing customer_id
                                Decimal(order.find('ns:Total', namespaces=ns).text)
                            )
                            order_data_batch.append(order_data)

                            lines = order.find('ns:Lines', namespaces=ns)
                            if lines is not None:
                                for line in lines.findall('ns:OrderLine', namespaces=ns):
                                    line_data = (
                                        int(line.find('ns:OrderLineId', namespaces=ns).text),
                                        int(order.find('ns:OrderId', namespaces=ns).text),
                                        int(line.find('ns:Qty', namespaces=ns).text),
                                        Decimal(line.find('ns:Price', namespaces=ns).text),
                                        int(line.find('ns:ProductId', namespaces=ns).text)
                                    )
                                    order_line_data_batch.append(line_data)

                    customer.clear()

                    # Commit the data in chunks
                    chunk_size += 1
                    if chunk_size >= num_chunks:
                        # Perform batch insert
                        if customer_data_batch:
                            insert_customer_batch(customer_data_batch, cursor)
                        if order_data_batch:
                            insert_order(order_data_batch, cursor)
                        if order_line_data_batch:
                            insert_order_line(order_line_data_batch, cursor)

                        conn.commit()  # Commit after each chunk
                        customer_data_batch.clear()
                        order_data_batch.clear()
                        order_line_data_batch.clear()
                        chunk_size = 0

                # Final commit if any data remains
                if customer_data_batch:
                    insert_customer_batch(customer_data_batch, cursor)
                if order_data_batch:
                    insert_order(order_data_batch, cursor)
                if order_line_data_batch:
                    insert_order_line(order_line_data_batch, cursor)

                conn.commit()

            print("Data import completed successfully!")

    except Exception as e:
        print(f"Error occurred during import: {e}")
        raise

def insert_customer_batch(customer_data_batch: list, cursor) -> None:
    insert_query = """
        INSERT INTO Customers (CustomerId, Name, Email, Age)
        VALUES (?, ?, ?, ?)
    """
    cursor.executemany(insert_query, customer_data_batch)






# from lxml import etree
# from decimal import Decimal
# from queries.queries import query

# def sanitize_value(value):
#     return value.replace("'", "''") if value else ""

# def insert_customer_if_not_exists(customer_data: dict) -> int:
#     customer_id = customer_data['customer_id']
#     name = sanitize_value(customer_data['name'])
#     email = sanitize_value(customer_data['email'])
#     age = customer_data['age']
#     # print(f"Sanitized Data: Customer ID: {customer_id}, Name: {name}, Email: {email}, Age: {age}")

#     check_query = f"""
#         SELECT CustomerId
#         FROM Customers
#         WHERE Email = '{email}'
#     """

#     result = query(check_query)

#     if result:
#         existing_customer_id = result[0][0]
#         print(f"Customer found with ID {existing_customer_id}")
#         return existing_customer_id

#     insert_query = f"""
#         INSERT INTO Customers (CustomerId, Name, Email, Age)
#         VALUES ({customer_id}, 
#                 '{name}', 
#                 '{email}', 
#                 {age})
#     """
#     # print(f"Executing insert query: {insert_query}")  # Debug print
#     query(insert_query)
#     # print(f"Customer {name} inserted")
#     return customer_id

# def insert_order(order_data: dict) -> int:
#     insert_query = f"""
#         INSERT INTO Orders (OrderId, CustomerId, Total)
#         VALUES ({order_data['order_id']}, 
#                 {order_data['customer_id']}, 
#                 {order_data['total']})
#     """
#     return query(insert_query)

# def insert_order_line(line_data: dict) -> int:
#     insert_query = f"""
#         INSERT INTO OrderLines (OrderLineId, OrderId, Qty, Price, ProductId)
#         VALUES ({line_data['order_line_id']},
#                 {line_data['order_id']},
#                 {line_data['qty']},
#                 {line_data['price']},
#                 {line_data['product_id']})
#     """
#     return query(insert_query)

# def parse_and_load_xml(xml_file_path: str) -> None:
#     try:
#         with open(xml_file_path, 'rb') as xml_file:
#             context = etree.iterparse(xml_file, events=('end',), tag='{http://schemas.datacontract.org/2004/07/DataGenerator}Customer')

#             ns = {'ns': 'http://schemas.datacontract.org/2004/07/DataGenerator'}

#             for event, customer in context:
#                 customer_data = {
#                     'customer_id': int(customer.find('ns:CustomerId', namespaces=ns).text),
#                     'name': customer.find('ns:Name', namespaces=ns).text.replace("'", "''"),
#                     'email': customer.find('ns:Email', namespaces=ns).text.replace("'", "''"),
#                     'age': int(customer.find('ns:Age', namespaces=ns).text)
#                 }

#                 customer_id = insert_customer_if_not_exists(customer_data)

#                 orders = customer.find('ns:Orders', namespaces=ns)
#                 if orders is not None:
#                     for order in orders.findall('ns:Order', namespaces=ns):
#                         order_data = {
#                             'order_id': int(order.find('ns:OrderId', namespaces=ns).text),
#                             'customer_id': customer_id,  # Use the existing customer_id
#                             'total': Decimal(order.find('ns:Total', namespaces=ns).text)
#                         }
                        
#                         insert_order(order_data)
#                         # print(f"Order {order_data['order_id']} inserted")
                        
#                         lines = order.find('ns:Lines', namespaces=ns)
#                         if lines is not None:
#                             for line in lines.findall('ns:OrderLine', namespaces=ns):
#                                 line_data = {
#                                     'order_line_id': int(line.find('ns:OrderLineId', namespaces=ns).text),
#                                     'order_id': order_data['order_id'],
#                                     'qty': int(line.find('ns:Qty', namespaces=ns).text),
#                                     'price': Decimal(line.find('ns:Price', namespaces=ns).text),
#                                     'product_id': int(line.find('ns:ProductId', namespaces=ns).text)
#                                 }
                                
#                                 insert_order_line(line_data)
#                                 # print(f"Order line {line_data['order_line_id']} inserted")

#                 customer.clear()

#             print("Data import completed successfully!")

#     except Exception as e:
#         print(f"Error occurred during import: {e}")
#         raise

# import concurrent.futures
# from lxml import etree
# from decimal import Decimal
# from queries.queries import query

# def insert_customer(customer_data: dict) -> int:
#     insert_query = f"""
#         INSERT INTO Customers (CustomerId, Name, Email, Age)
#         VALUES ({customer_data['customer_id']}, 
#                 '{customer_data['name']}', 
#                 '{customer_data['email']}', 
#                 {customer_data['age']})
#     """
#     return query(insert_query)

# def insert_order(order_data: dict) -> int:
#     insert_query = f"""
#         INSERT INTO Orders (OrderId, CustomerId, Total)
#         VALUES ({order_data['order_id']}, 
#                 {order_data['customer_id']}, 
#                 {order_data['total']})
#     """
#     return query(insert_query)

# def insert_order_line(line_data: dict) -> int:
#     insert_query = f"""
#         INSERT INTO OrderLines (OrderLineId, OrderId, Qty, Price, ProductId)
#         VALUES ({line_data['order_line_id']},
#                 {line_data['order_id']},
#                 {line_data['qty']},
#                 {line_data['price']},
#                 {line_data['product_id']})
#     """
#     return query(insert_query)

# def process_customer_data(customer_data: dict):
#     # Insert customer data
#     insert_customer(customer_data)
#     print(f"Customer {customer_data['name']} inserted")
    
#     # Insert orders and order lines
#     for order_data in customer_data['orders']:
#         insert_order(order_data)
#         for line_data in order_data['lines']:
#             insert_order_line(line_data)

# def parse_and_load_xml(xml_file_path: str) -> None:
#     with open(xml_file_path, 'rb') as xml_file:
#         context = etree.iterparse(xml_file, events=('end',), tag='{http://schemas.datacontract.org/2004/07/DataGenerator}Customer')
#         ns = {'ns': 'http://schemas.datacontract.org/2004/07/DataGenerator'}

#         customer_data_list = []
        
#         for event, customer in context:
#             customer_data = {
#                 'customer_id': int(customer.find('ns:CustomerId', namespaces=ns).text),
#                 'name': customer.find('ns:Name', namespaces=ns).text.replace("'", "''"),
#                 'email': customer.find('ns:Email', namespaces=ns).text.replace("'", "''"),
#                 'age': int(customer.find('ns:Age', namespaces=ns).text),
#                 'orders': []  # This will hold the orders for the customer
#             }

#             # Collect order data
#             orders = customer.find('ns:Orders', namespaces=ns)
#             if orders is not None:
#                 for order in orders.findall('ns:Order', namespaces=ns):
#                     order_data = {
#                         'order_id': int(order.find('ns:OrderId', namespaces=ns).text),
#                         'customer_id': customer_data['customer_id'],
#                         'total': Decimal(order.find('ns:Total', namespaces=ns).text),
#                         'lines': []  # This will hold the order lines
#                     }

#                     # Collect order line data
#                     lines = order.find('ns:Lines', namespaces=ns)
#                     if lines is not None:
#                         for line in lines.findall('ns:OrderLine', namespaces=ns):
#                             line_data = {
#                                 'order_line_id': int(line.find('ns:OrderLineId', namespaces=ns).text),
#                                 'order_id': order_data['order_id'],
#                                 'qty': int(line.find('ns:Qty', namespaces=ns).text),
#                                 'price': Decimal(line.find('ns:Price', namespaces=ns).text),
#                                 'product_id': int(line.find('ns:ProductId', namespaces=ns).text)
#                             }
#                             order_data['lines'].append(line_data)

#                     customer_data['orders'].append(order_data)
                
#             customer_data_list.append(customer_data)

#             # Clear the customer element to free memory
#             customer.clear()

#         # Concurrently process customer data using ThreadPoolExecutor
#         with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
#             # Submit tasks to the executor
#             futures = [executor.submit(process_customer_data, customer_data) for customer_data in customer_data_list]

#             # Wait for all tasks to complete
#             for future in concurrent.futures.as_completed(futures):
#                 future.result()  # Will raise exceptions if any occurred during processing

#         print("Data import completed successfully!")

