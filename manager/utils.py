import os
import pandas as pd
import shutil

def split_large_csv(file_path: str, output_folder: str, max_rows = 1_000_000):
    """
    Splits a large CSV file into multiple files, each containing a maximum of 1 million rows.

    Parameters:
        file_path (str): Path to the original CSV file.
        output_folder (str): Path to the folder where the split CSV files will be saved.
        max_rows (int) : Maximum rows per split file.

    Returns:
        List[str]: List of paths to the newly created CSV files.
    """
    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)

    total_rows = sum(1 for _ in open(file_path)) - 1  # Subtract 1 for the header

    # No need to split, copy it to the output folder
    if total_rows <= max_rows:
        output_file = os.path.join(output_folder, os.path.basename(file_path))
        shutil.copy(file_path, output_file)
        return [output_file]

    base_name = os.path.splitext(os.path.basename(file_path))[0]
    output_files = []

    chunks = pd.read_csv(file_path, chunksize=max_rows)
    for idx, chunk in enumerate(chunks):
        split_file_path = os.path.join(output_folder, f"{base_name}_chunk_{idx + 1}.csv")
        chunk.to_csv(split_file_path, index=False)
        output_files.append(split_file_path)

    return output_files

def split_csvs_in_directory(input_dir, output_dir):
    """
    Iterates over all CSV files in the provided directory and splits them into smaller files
    in a common output directory.

    Parameters:
        input_dir (str): Path to the directory containing CSV files.
        output_dir (str): Path to the directory where split files will be saved.

    Returns:
        List[str]: List of paths to all newly created CSV files.
    """

    os.makedirs(output_dir, exist_ok=True)

    all_split_files = []

    for file_name in os.listdir(input_dir):
        file_path = os.path.join(input_dir, file_name)
        # Process only CSV files
        if os.path.isfile(file_path) and file_name.lower().endswith('.csv'):
            split_files = split_large_csv(file_path, output_dir)
            all_split_files.extend(split_files)

    return all_split_files


if __name__ == '__main__':
    split_files = split_csvs_in_directory("data", "data/out")
    print(split_files)