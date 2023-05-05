import csv

# This was only used to duplicate the dataset

# Define the input and output file paths
input_file = '../dataset/ratings.txt'
output_file = './expanded_data.txt'

# Define the range of userids to use for the new users
# new_user_id_range = range(100000, 200000)

last_user_id = 283228
last_seen_user_id = 0

# Open the input and output CSV files
with open(input_file, 'r') as csv_in, open(output_file, 'w', newline='') as csv_out:
    reader = csv.reader(csv_in)
    writer = csv.writer(csv_out)

    # Write the header row to the output CSV file
    header = next(reader)
    writer.writerow(header)
    new_user_id_offset = 0
    # Loop through the original rows and create duplicates with new userids
    for row in reader:
        # Get the original userid, movieid, and rating
        userid = row[0]
        movieid = row[1]
        rating = row[2]

        # Generate a new userid for the duplicate row
        if last_seen_user_id != userid:
            last_seen_user_id = userid
            new_user_id_offset += 1

        # Write the original row to the output CSV file
        writer.writerow(row)

        # Write the duplicate row with the new userid to the output CSV file
        for i in range(3):
            duplicate_row = [str(new_user_id_offset + last_user_id*(1+i)), movieid, rating, 0]
            writer.writerow(duplicate_row)
