import queries.queries as queries
import queries.creation_queries as creation_queries
import parser, time

def get_runtime(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        print(f'--- {(time.time() - start_time)} seconds ---')
        return result
    return wrapper

@get_runtime
def main():
    creation_queries.reset_tables()

    print("Starting XML processing...")
    parser.parse_and_load_xml("customers.xml", num_chunks=1000)
    print("XML processing finished.")
    
if __name__ == "__main__":
    main()

# How many rows are imported for each of the tables?
# What is the average, minimum, & maximum amount of lines per order?
# What customer placed the most orders and how many orders were placed?
# What customer placed the most orderLines  and how many ordersLines were ordered?
# What product is the most popular and how many times has it been sold
# E.g. Person1 bought 5 of them, Person2 bought 1 of them, the answer is 2. 