import csv

# Define the schema of your fixed-length file
# Each tuple represents (field name, start position, end position)
schema = [
    ("Name", 0, 10),  # First 10 characters represent Name
    ("ID", 10, 15),   # Characters 11-15 represent ID
    ("Age", 15, 17),  # Characters 16-17 represent Age
    # Add more fields as needed
]

# Function to parse each line based on the fixed-length schema
def parse_fixed_length_line(line, schema):
    parsed_data = {}
    for field_name, start, end in schema:
        parsed_data[field_name] = line[start:end].strip()  # Strip leading/trailing spaces
    return parsed_data

# Input fixed-length file and output CSV file
input_file = "fixed_length_data.txt"
output_file = "parsed_data.csv"

# Read the fixed-length file, parse it, and save as CSV
with open(input_file, "r") as infile, open(output_file, "w", newline="", encoding="utf-8") as outfile:
    writer = csv.DictWriter(outfile, fieldnames=[field[0] for field in schema], quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()

    for line in infile:
        parsed_line = parse_fixed_length_line(line, schema)
        writer.writerow(parsed_line)

print(f"Parsed data saved to {output_file}")
