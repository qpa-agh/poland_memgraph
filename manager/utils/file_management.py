import os
import pandas as pd
import shutil
import gc


def find_file(name):
    try:
        filename = next(
            file_name
            for file_name in os.listdir("/data")
            if name in file_name and file_name.lower().endswith(".csv")
        )
    except StopIteration as e:
        print(f"No such file as {name}", flush=True)
        raise e
    return filename


def prepare_paths(name, filename, clear_output):
    source_filepath = os.path.join("/data", filename)
    output_dir = os.path.join("/data", name)
    if os.path.exists(output_dir) and clear_output:
        shutil.rmtree(output_dir)
        if os.path.exists(output_dir):
            os.rmdir(output_dir)
    return source_filepath, output_dir


def prepare_files(name, dataframe_modifier=None, max_rows=1_000_000, clear_output=False):
    print(name)
    filename = find_file(name)
    source_filepath, output_dir = prepare_paths(name, filename, clear_output)
    if os.path.exists(output_dir) and not clear_output:
        return output_dir
    if os.path.exists(output_dir) and clear_output:
        raise BaseException("Did not clear output properly")

    split_large_csv(
        source_filepath,
        output_dir,
        dataframe_modifier=dataframe_modifier,
        max_rows=max_rows,
    )
    return output_dir


def split_large_csv(
    file_path: str, output_folder: str, max_rows=1_000_000, dataframe_modifier=None
):
    """
    Splits a large CSV file into multiple files, each containing a maximum of 1 million rows.

    Parameters:
        file_path (str): Path to the original CSV file.
        output_folder (str): Path to the folder where the split CSV files will be saved.
        max_rows (int) : Maximum rows per split file.

    Returns:
        List[str]: List of paths to the newly created CSV files.
    """
    os.makedirs(output_folder, exist_ok=True)

    output_files = []

    chunks = pd.read_csv(file_path, chunksize=max_rows)
    for idx, chunk in enumerate(chunks):
        os.makedirs(os.path.join(output_folder), exist_ok=True)
        split_file_path = os.path.join(output_folder, f"chunk_{idx + 1}.csv")
        if dataframe_modifier is not None:
            chunk = dataframe_modifier(chunk)
        chunk.to_csv(split_file_path, index=False)
        output_files.append(split_file_path)
        del chunk
        gc.collect()

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
        if os.path.isfile(file_path) and file_name.lower().endswith(".csv"):
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            split_files = split_large_csv(
                file_path, os.path.join(output_dir, base_name)
            )
            all_split_files.extend(split_files)

    return all_split_files


if __name__ == "__main__":
    split_files = split_csvs_in_directory("data", "data/out")
    print(split_files)
