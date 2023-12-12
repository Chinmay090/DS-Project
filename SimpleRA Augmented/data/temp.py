import random

def generate_random_row(num_columns, column_ranges):
    row = [random.randint(start, end) for start, end in column_ranges]
    return row

def generate_random_rows(num_rows, num_columns, column_ranges):
    rows = [[i] + generate_random_row(num_columns, column_ranges) for i in range(*num_rows)]
    return rows

# Example usage:
num_rows = (30,201)
num_columns = 3
column_ranges = [(13, 20), (0,1), (1,10)]

random_rows = generate_random_rows(num_rows, num_columns, column_ranges)

for row in random_rows:
    for i ,r in enumerate(row):
        print(r, end=('\n' if i==num_columns else ', '))
