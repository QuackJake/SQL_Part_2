from db_connection import create_connection

def query(str_query):
    with create_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(str_query)
            if str_query.strip().upper().startswith('SELECT'):
                return cursor.fetchall()
            else:
                conn.commit()
                return cursor.rowcount

def fetch_all_data(table_name):
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    conn.close()
    return rows