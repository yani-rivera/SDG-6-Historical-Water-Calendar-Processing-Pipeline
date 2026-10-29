import os
import pandas as pd
from glob import glob
import logging
import argparse

# Set up logging for better visibility
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_args():
    """
    Parses command-line arguments to allow specifying input base directory, output base directory,
    and optionally a specific year directory to process.
    """
    parser = argparse.ArgumentParser(description="Generate QA report for missing group_uid values in CSV files.")

    # Required arguments
    parser.add_argument("input_base_dir", help="Base directory containing year subdirectories.")
    parser.add_argument("output_base_dir", help="Base directory to store the output reports.")
    parser.add_argument("group_catalog_file", help="Path to the group catalog file (CSV) with 'grp_canon' and 'grp_uid'.")

    # Optional argument to specify a single year
    parser.add_argument("--year", help="Optional: Specify a specific year directory to process.", required=False)

    return parser.parse_args()

def generate_report(input_base_dir, output_base_dir, group_catalog_df, year=None):
    """
    Processes all CSV files in the directory (for each year), identifying rows without group_uid,
    and generates a report CSV file in the 'data/qa' directory for each year.
    If a specific year is provided, only that year will be processed.
    """
    # If a year is specified, process only that year's directory
    if year:
        year_dirs = [os.path.join(input_base_dir, year, "calproc_gpuid")]
    else:
        # Get all year directories (e.g., data/2022/, data/2023/, etc.)
        year_dirs = glob(os.path.join(input_base_dir, "*", "calproc_gpuid"))
    
    if not year_dirs:
        logging.error("No year directories found in the base directory.")
        return

    # Iterate over each year directory
    for year_dir in year_dirs:
        logging.info(f"Processing year directory: {year_dir}")
        
        # Get all the CSV files under the current year directory
        all_files = glob(os.path.join(year_dir, "*.csv"))
        valid_files = [f for f in all_files if not f.endswith('~$')]  # Avoid temporary files

        if not valid_files:
            logging.warning(f"No valid CSV files found in {year_dir}")
            continue

        # Prepare the output directory for the QA report (for each year)
        output_qa_dir = os.path.join(output_base_dir, "qa", os.path.basename(os.path.dirname(year_dir)))
        
        #os.makedirs(output_qa_dir, exist_ok=True)

        # Initialize a list to store rows with missing group_uid
        missing_group_uid_rows = pd.DataFrame()  # Empty DataFrame to hold missing rows

        # Process each file in the current year directory
        for file in valid_files:
            logging.debug(f"Processing file: {file}")  # Debug output

        
            # Load the CSV file into a DataFrame
            df = pd.read_csv(file, dtype=str, encoding="utf-8-sig")

                # Check if the required 'group_uid' column exists
                # if "group_uid" not in df.columns:
                #     logging.warning(f"Skipping {file}: 'group_uid' column missing")
                #     continue

                # Filter rows where 'group_uid' is missing

            missing_rows = df[df['group_uid'].isna()]

                # Add these rows to the list
               
            missing_group_uid_rows = pd.concat([missing_group_uid_rows, missing_rows], ignore_index=True)


        # Concatenate all the rows with missing group_uid into a single DataFrame

           

            # Dynamically generate the report file name based on the year
            
    missing_group_uid_rows.to_csv('data/qa/'+year+'_missing.csv', index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    # Parse the command-line arguments
    args = parse_args()

    input_base_dir = args.input_base_dir  # Base directory containing the year directories
    output_base_dir = args.output_base_dir  # Directory to store the reports
    group_catalog_file = args.group_catalog_file  # Path to the group catalog file

    # Load the group catalog file into a DataFrame with utf-8-sig encoding
    try:
        group_catalog_df = pd.read_csv(group_catalog_file, encoding="utf-8-sig")
        group_catalog_df.columns = group_catalog_df.columns.str.strip()  # Clean column names
    except Exception as e:
        logging.error(f"Error loading group catalog file: {e}")
        exit()

    # Ensure the catalog contains 'grp_canon' and 'grp_uid' columns
    # if 'grp_canon' not in group_catalog_df.columns or 'grp_uid' not in group_catalog_df.columns:
    #     logging.error("The group catalog file must contain 'grp_canon' and 'grp_uid' columns.")
    #     exit()

    # Generate the report (with optional year filter)
    generate_report(input_base_dir, 'data/qa/'+args.year, group_catalog_df, year=args.year)