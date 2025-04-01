# BDA Data Preprocessing and Visualization

## Overview
This repository contains a PySpark-based data preprocessing and visualization script designed for analyzing admission inquiry data. The script performs data cleaning, preprocessing, and generates visual insights to help understand relationships and trends in the dataset.

## Features
- **Data Cleaning**: Handles missing values, invalid IDs, and repetitive text in columns.
- **Preprocessing**: Converts date columns to a standard format, deduplicates source data, and standardizes text fields.
- **Visualizations**:
  - Heatmap showing the relationship between `Status` and `Source`.
  - Bar charts for top 10 states and cities based on lead status.
  - Horizontal bar chart for the most frequent comments in the `2nd last Comment` column.

## Requirements
- Python 3.8 or higher
- PySpark
- Pandas
- Matplotlib
- Seaborn

## Usage
1. Place the input CSV file in the specified directory (`C:/Users/molaw/code/PA1/input/FILE1.csv`).
2. Run the script:
   ```bash
   python src/my_pyspark_script.py
   ```

## Data Preprocessing Steps
Step 1: find and create csv with distinct values from each column
step 2: manually identify pre-processing required and apply in function Handle Missing Values:

Identify columns or rows with missing (null or NaN) values.
Replace missing values with default values (e.g., "Unknown" for strings, 0 for numbers) or remove rows/columns with too many missing values.
Standardize Data Types:

Ensure all columns have consistent and appropriate data types (e.g., integers for numeric columns, strings for categorical columns).
Convert columns to the correct data type if necessary.
Normalize or Scale Data:

For numeric columns, normalize or scale the values to bring them into a consistent range (e.g., 0 to 1) for better performance in machine learning models.
Remove Duplicates:

Identify and remove duplicate rows to ensure the data is unique and clean.
Trim or Clean Strings:

Remove leading/trailing whitespace from string columns.
Standardize text formatting (e.g., capitalize the first letter of each word).
Filter Invalid Data:

Remove rows with invalid or out-of-range values (e.g., negative ages, invalid dates).
Feature Engineering:

Create new columns based on existing ones (e.g., extracting the year from a date column).
Combine or split columns as needed.
Reorder or Rename Columns:

Rename columns to more meaningful names.
Reorder columns for better readability or consistency.
Log or Handle Errors:

Log any errors or issues encountered during preprocessing.
Handle problematic rows gracefully without crashing the process.
Return the Cleaned DataFrame:

After applying all preprocessing steps, return the cleaned and prepared DataFrame for further analysis or processing.