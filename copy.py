import pymysql.cursors

# Local database connection details
local_connection = pymysql.connect(
    host='localhost',
    user='root',
    password='sravani',
    database='edu_tech',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

# Server database connection details
server_connection = pymysql.connect(
    host='13.53.253.163',
    user='tavva',
    password='tavva123',
    database='edu_site',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

def copy_table_data(local_table_name, server_table_name, batch_size=1000):
    try:
        with local_connection.cursor() as local_cursor, server_connection.cursor() as server_cursor:
            # Get the total number of rows in the local table
            local_cursor.execute(f"SELECT COUNT(*) as total_rows FROM {local_table_name}")
            total_rows = local_cursor.fetchone()['total_rows']

            # Calculate the number of batches
            num_batches = (total_rows + batch_size - 1) // batch_size

            # Copy data in batches
            for i in range(num_batches):
                offset = i * batch_size
                local_cursor.execute(f"SELECT * FROM {local_table_name} LIMIT {offset}, {batch_size}")
                records = local_cursor.fetchall()

                # Truncate server table to avoid duplicate data
                if offset == 0:
                    server_cursor.execute(f"TRUNCATE TABLE {server_table_name}")

                # Insert records into server table in bulk
                if records:
                    placeholders = ', '.join(['%s'] * len(records[0]))
                    columns = ', '.join(records[0].keys())
                    values = ', '.join(['(' + ', '.join(['%s'] * len(record)) + ')' for record in records])
                    insert_sql = f"INSERT INTO {server_table_name} ({columns}) VALUES {values}"
                    server_cursor.execute(insert_sql, [value for record in records for value in record.values()])

                print(f"Batch {i+1}/{num_batches} copied.")

        # Commit the changes to the server database
        server_connection.commit()
        print(f"Data from {local_table_name} copied to {server_table_name} successfully.")
    except Exception as e:
        print(f"Error copying data: {e}")
        local_connection.rollback()  # Rollback changes if an error occurs

if __name__ == "__main__":
    # Copy data from local udise_schools to server udise_schools_backup
    copy_table_data('udise_schools', 'udise_schools_backup', batch_size=1000)
    
    # Close connections
    local_connection.close()
    server_connection.close()
