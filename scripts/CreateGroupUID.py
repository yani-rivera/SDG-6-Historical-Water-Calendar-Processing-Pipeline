import pandas as pd
import argparse

def generate_group_uid(df, create_for_blanks=False):
    """
    Generates a 'group_uid' column by combining water_sector (numeric), water_source (mnemonic), 
    distribution_unit (mnemonic), with an optional sequential number for each unique combination.
    
    Args:
    - df (pd.DataFrame): The DataFrame containing the data.
    - create_for_blanks (bool): Flag to create UID even for rows with blanks in 'raw_group'.
    
    Returns:
    - pd.DataFrame: The updated DataFrame with the 'group_uid' column.
    """
    # Ensure sector is zero-padded to 3 digits (e.g., 001, 002, ...)
    df['sector'] = df['water_sector'].apply(lambda x: str(int(x)).zfill(3))
    
    # Create mnemonic parts for source and distribution unit
    df['source_mnemonic'] = df['water_source'].str[:3].str.upper()
    df['dist_mnemonic'] = df['distribution_unit'].str[:3].str.upper()
    
    # Create a combined identifier for each group (sector, source, dist)
    df['group_key'] = df['sector'] + '-' + df['source_mnemonic'] + '-' + df['dist_mnemonic']
    
    # Generate sequential IDs for each unique 'group_key'
    df['group_uid_seq'] = df.groupby('group_key').ngroup() + 1
    
    # Create final UID format: sector-source-mnemonic-dist-sequential
    df['group_uid_new'] = df['sector'] + '-' + df['source_mnemonic'] + '-' + df['dist_mnemonic'] + '-' + df['group_uid_seq'].astype(str)
    #df['group_uid'] = df['sector'] + '-' + df['source_mnemonic'] + '-' + df['dist_mnemonic'] + '-' + df['group_uid_seq'].astype(str)
    
    # Drop the extra sequential column
    df.drop(columns=['group_uid_seq'], inplace=True)
    
    return df

def main(input_file, output_file, create_for_blanks=False):
    # Read the input CSV file with utf-8-sig encoding
    df = pd.read_csv(input_file, encoding='utf-8-sig')
    
    # Generate the group_uid column
    df = generate_group_uid(df, create_for_blanks)
    
    # Save the updated DataFrame to the output file with utf-8-sig encoding
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"File with generated 'group_uid' saved to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate group_uid for water distribution data.")
    parser.add_argument("--input", required=True, help="Path to the input CSV file.")
    parser.add_argument("--output", required=True, help="Path to save the output CSV file.")
    parser.add_argument("--create_for_blanks", action="store_true", help="Flag to create UID for rows with blank 'raw_group'.")
    
    args = parser.parse_args()
    
    # Run the main function with arguments
    main(args.input, args.output, args.create_for_blanks)
