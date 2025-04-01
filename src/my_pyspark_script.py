from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import re

# Initialize Spark session
spark = SparkSession.builder.appName("AdmissionInquiryAnalysis").getOrCreate()

# Load the dataset
file_path = "C:/Users/molaw/code/PA1/input/FILE1.csv"  # Adjust this as necessary
df_spark = spark.read.option("header", True).option("inferSchema", True).csv(file_path)

# Convert Spark DataFrame to Pandas DataFrame
df_pandas = df_spark.toPandas()

# Define helper functions
def remove_repetition(input_string):
    """Remove repeated words in a string."""
    return re.sub(r'\b(\w+)\b(?:\s+\1\b)+', r'\1', input_string.strip(), flags=re.IGNORECASE)

def clean_and_deduplicate_source(input_string):
    """Clean and deduplicate the 'Source' column."""
    values = input_string.split('|')
    unique_values = set(value.strip() for value in values if value.strip() != "Not Known")
    return ' | '.join(unique_values)

# Preprocess the Pandas DataFrame
def preprocess_dataframe(df):
    # Fill missing values
    df.fillna({"State": "Unknown", "City": "Unknown", "Last Comment": "Unknown", "2nd last Comment": "Unknown"}, inplace=True)

    # Convert date columns
    for col_name in ['CreatedOn', 'UpdatedOn']:
        if col_name in df.columns:
            df[col_name] = pd.to_datetime(df[col_name], format='%Y-%m-%d', errors='coerce')

    # Apply preprocessing logic for specific columns
    for index, row in df.iterrows():
        if "ID" in row and (pd.isnull(row["ID"]) or not str(row["ID"]).isdigit()):
            df.at[index, "ID"] = -1  # Replace invalid IDs with -1
        if "2nd last Comment" in row and isinstance(row["2nd last Comment"], str):
            formatted_value = row["2nd last Comment"].replace("\\", "").replace("\"", "")
            formatted_value = formatted_value.replace("capture_request_id:", "capture_request_id:")
            df.at[index, "2nd last Comment"] = formatted_value
        if "Applicant Name" in row and isinstance(row["Applicant Name"], str):
            value = row["Applicant Name"].lower()
            value = remove_repetition(value)
            value = re.sub(r'[?_]', '', value)
            if not value.strip():
                value = "Unknown"
            df.at[index, "Applicant Name"] = value
        if "Status" in row and isinstance(row["Status"], str):
            df.at[index, "Status"] = row["Status"].strip().title()
        if "Source" in row and isinstance(row["Source"], str):
            df.at[index, "Source"] = clean_and_deduplicate_source(row["Source"])

    return df

# Apply preprocessing to the Pandas DataFrame
df_pandas = preprocess_dataframe(df_pandas)

# Analysis 1: Relationship between Status and Source
if {'Status', 'Source'}.issubset(df_pandas.columns):
    status_source_counts = df_pandas.groupby(['Status', 'Source']).size().unstack().fillna(0)
    plt.figure(figsize=(12, 6))
    sns.heatmap(status_source_counts, cmap='coolwarm', annot=False, linewidths=0.5)
    plt.title('Status vs. Source Heatmap')
    plt.show()
else:
    print("Error: 'Status' or 'Source' column missing from data")

# Analysis 2: State and City wise lead status analysis
for col in ['State', 'City']:
    if {col, 'Status'}.issubset(df_pandas.columns):
        # Group by column and 'Status', then count the size
        status_counts = df_pandas.groupby([col, 'Status']).size().unstack().fillna(0)

        # Sort by total size across all statuses and take the top 10
        top_10 = status_counts.sum(axis=1).nlargest(10).index
        status_counts = status_counts.loc[top_10]

        # Plot the results
        status_counts.plot(kind='bar', stacked=True, figsize=(15, 6))
        plt.title(f'Top 10 {col}-wise Lead Status Analysis')
        plt.xlabel(col)
        plt.ylabel('Number of Leads')
        plt.legend(title='Status')
        plt.show()
    else:
        print(f"Error: '{col}' or 'Status' column missing from data")

# Analysis 3: Insights from 2nd Last Comment Column
if '2nd last Comment' in df_pandas.columns:
    df_pandas['2nd last Comment'].value_counts().head(20).plot(kind='barh', figsize=(10, 6))
    plt.title('Top 20 Most Frequent 2nd Last Comments')
    plt.xlabel('Frequency')
    plt.ylabel('Comment')
    plt.show()
else:
    print("Error: '2nd last Comment' column missing from data")

# Save the preprocessed data
df_pandas.to_csv("preprocessed_FILE1.csv", index=False)
print("Preprocessed data saved to 'preprocessed_FILE1.csv'")
