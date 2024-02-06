import io
import psycopg2
import csv

# Open the original CSV file for reading
with open('F:/postgresql_training1_ati/scripts/data_transformation.csv', 'r') as csvfile:
    # Create a CSV reader
    csv_reader = csv.reader(csvfile)

    # Create a new list to store modified rows
    modified_rows = []

    # Iterate over each row, add '|' after the characters and append to the new list
    for row in csv_reader:
        modified_row = [
            row[0][:5] + '|' + row[0][5:15] + '|' + row[0][15:21] + '|' + row[0][22:27] + '|' + row[0][27:37] + '|' + row[0][37:]]
        # modified_row += row[1:]
        modified_rows.append(modified_row)
    print(modified_row)


# Open a new CSV file for writing
with open('F:/postgresql_training1_ati/scripts/data_transformed.csv', 'w', newline='') as csvfile:
    # Create a CSV writer
    csv_writer = csv.writer(csvfile, delimiter='|', lineterminator="\n", quoting=csv.QUOTE_MINIMAL)

    # Write the modified rows to the new CSV file
    csv_writer.writerows(modified_rows)

# Connect to PostgreSQL
conn = psycopg2.connect(database="postgres", user="postgres", password="pgadmin", host="localhost", port="5432")
# Create a cursor
cur = conn.cursor()

# Create the table if not exists
create_table = '''CREATE TABLE IF NOT EXISTS product (
    pid char(7),
    pname char(13),
    pcategory char(8),
    pprice decimal(6,0),
    ppurchase_date date,
    ppurchase_by char(12)
);'''
cur.execute(create_table)

# Read data from data_transformed.csv file and load into table product
with open('F:/postgresql_training1_ati/scripts/data_transformed.csv', 'r') as csvfile:
    csvfile.seek(0)
    csv_reader = csv.reader(csvfile, delimiter='|')
    cur.copy_from(csvfile, 'product', sep='|')

# Select data from the product table
select_product = 'SELECT * FROM product;'
cur.execute(select_product)
rows = cur.fetchall()
for row in rows:
    print(row)

# Commit changes and close the connection
conn.commit()
cur.close()
conn.close()
