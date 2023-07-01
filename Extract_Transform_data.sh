#!/bin/bash

echo "Unzip the data"
tar -xvzf tolldata.tgz

echo "Extract data from csv file"
cut -d"," -f1-4 vehicle-data.csv > /home/project/airflow/dags/csv_data.csv

echo "Extract data from tsv file"
cut -d -f5-7 tollplaza-data.tsv > /home/project/airflow/dags/tsv_data.tsv

echo "Extract data from fixed width file"
awk "NF{print $(NF-1),$NF}" OFS="\t" payment-data.txt > /home/project/airflow/dags/fixed_width_data.csv

echo "Consolidate data extracted from previous tasks"
paste csv_data.csv tsv_data.tsv fixed_width_data.csv > /home/project/airflow/dags/extracted_data.csv

echo "Transform and load the data"
awk "$5 = toupper($5)" < extracted_data.csv > transformed_data.csv
