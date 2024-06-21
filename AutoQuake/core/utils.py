import csv
from datetime import datetime, timedelta
import pandas as pd

def convert_txt_to_csv(txt_file_path, csv_file_path, headers):
    """
    Read a space-separated TXT file, add corresponding fields, and convert to CSV format
    :param txt_file_path: Path to the space-separated TXT file
    :param csv_file_path: Path to the converted CSV file
    :param headers: Headers for the CSV file
    """
    with open(txt_file_path, 'r') as txt_file, open(csv_file_path, 'w', newline='') as csv_file:
        reader = txt_file.readlines()
        writer = csv.writer(csv_file)
        writer.writerow(headers)  # Write CSV headers

        for line in reader:
            row = line.strip().split()
            writer.writerow(row)

        print(f"Converted {txt_file_path} to {csv_file_path} with headers {headers}")

def date_range(start_date, end_date):
    start_date = datetime.strptime(str(start_date), "%Y%m%d")
    end_date = datetime.strptime(str(end_date), "%Y%m%d")
    delta = end_date - start_date
    date_list = []
    for i in range(delta.days + 1):
        date = start_date + timedelta(days=i)
        date_list.append(date.strftime("%Y%m%d"))
    return date_list

def get_sta_list(station_path):
    df = pd.read_csv(station_path)
    return df['station'].to_list()