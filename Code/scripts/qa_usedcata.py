import argparse
import pandas as pd

# Function to compare catalog with aggregation file and generate report
def compare_catalog_to_aggregation(catalog_file, aggregation_file, output_report):
    # Read the catalog and aggregation files
    catalog = pd.read_csv(catalog_file, encoding="utf-8-sig")
    aggregation = pd.read_csv(aggregation_file, encoding="utf-8-sig")

    # Normalize the column names to ensure no issues with spaces or special characters
    catalog.columns = catalog.columns.str.strip()
    aggregation.columns = aggregation.columns.str.strip()

    # Assuming 'group_uid' is the common column to match
    unmatched = catalog[~catalog['group_uid'].isin(aggregation['group_uid'])]

    # Generate a report with the unmatched rows
    unmatched.to_csv(output_report, index=False, encoding="utf-8-sig")
    print(f"Report generated: {output_report}")
    return unmatched

# Function to parse CLI arguments
def parse_args():
    parser = argparse.ArgumentParser(description="Compare catalog and aggregation files, and generate a report of unmatched rows.")
    parser.add_argument("--catalog", required=True, help="Path to the catalog CSV file")
    parser.add_argument("--aggregation", required=True, help="Path to the WC aggregation CSV file")
    parser.add_argument("--output", required=True, help="Path to save the output report (CSV format)")
    return parser.parse_args()

# Main execution
if __name__ == "__main__":
    args = parse_args()

    # Compare the files and generate the report
    unmatched_rows = compare_catalog_to_aggregation(args.catalog, args.aggregation, args.output)

    # If you want to see the unmatched rows as a preview
    print(unmatched_rows.head())