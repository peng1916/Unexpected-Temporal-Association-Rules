#!/usr/bin/python

import csv

def read_csv(file_path, has_header = True, has_row_names = True, are_numerical_data = False):
    col_names = []
    row_names = []
    data = []
    with open(file_path, "r") as f:
        spamreader = csv.reader(f, delimiter=',', quotechar='\"')
        if has_header:
            row = spamreader.next()
            row = row[1:]
            row = [x.strip() for x in row]
            col_names.extend(row)
    with open(file_path, "r") as f:
        spamreader = csv.reader(f, delimiter = ',', quotechar = '\"')
        if has_header:
            next(spamreader, None)
        if has_row_names:
            for row in spamreader:
                row_names.append(row[0].strip())
                row = row[1:]
                row = [x.strip() for x in row]
                data.append(row)
        else:
            for row in spamreader:
                row = [x.strip() for x in row]
                data.append(row)
	if are_numerical_data:
            data = [[int(x) for x in row] for row in data]
    return col_names, row_names, data

def write_csv(file_path, data, header = None, row_names = None):
    with open(file_path, "w") as f:
        f.write("," + ",".join(header) + "\n")
        for i in range(0, len(data), 1):
            f.write(row_names[i] + "," + ",".join([str(x) for x in data[i]]) + "\n")

