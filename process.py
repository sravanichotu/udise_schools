import os
import sys
import hjson
import pymysql.cursors

# Connect to the database
server_connection = pymysql.connect(
    host='13.53.253.163',
    user='tavva',
    password='tavva123',
    database='edu_site',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

def get_data_files():
    raw_data_folder = "./raw/"
    l = os.listdir(raw_data_folder)
    l.sort()
    return l

def process_file(file_name):
    print("Processing {file_name}".format(file_name=file_name))
    source_raw_data_file_path = f"./raw/{file_name}"
    with open(source_raw_data_file_path, "r") as file_obj:
        file_data = file_obj.read()

    data = hjson.loads(file_data)
    insert_array = []
    invalid_records = []

    for feature in data["features"]:
        record = feature["properties"]
        try:
            # Attempt to convert "schcd" to integer
            record["schcd"] = int(record["schcd"])
        except ValueError:
            # If "schcd" cannot be converted to integer, add the record to invalid_records list
            print(f"Invalid 'schcd' value: {record['schcd']}. Inserting into invalid records table.")
            invalid_records.append(record)
            continue

        record["id"] = feature["id"]
        record["lon"] = feature["geometry"]["coordinates"][0]
        record["lat"] = feature["geometry"]["coordinates"][1]
        insert_array.append(record)

    with server_connection.cursor() as cursor:
        # Insert valid records into udise_schools table
        for record in insert_array:
            placeholders = ', '.join(['%s'] * len(record))
            columns = ', '.join(record.keys())
            sql = f"INSERT INTO udise_schools ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, list(record.values()))

        # Insert invalid records into udise_schools table
        for record in invalid_records:
            placeholders = ', '.join(['%s'] * len(record))
            columns = ', '.join(record.keys())
            sql = f"INSERT INTO udise_schools ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, list(record.values()))

    server_connection.commit()

def main():
    file_list = get_data_files()
    for file_name in file_list:
        process_file(file_name)

if __name__ == "__main__":
    main()
