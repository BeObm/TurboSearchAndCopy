import os
import glob
from concurrent.futures import ThreadPoolExecutor
import argparse
from tqdm import tqdm

def generate_file_paths(directory, file_type_extensions):
    for root, _, files in os.walk(directory):
        for ext in file_type_extensions:
            for file_path in glob.iglob(os.path.join(root, f'*{ext}')):
                yield file_path

def save_paths_to_file(file_paths, file_type, batch_count, destination_folder):
    save_path = os.path.join(destination_folder, f"{file_type}_{batch_count}.txt")
    with open(save_path, 'a') as file:
        for path in file_paths:
            file.write(f"{path}\n")

def process_file(file_path, current_batch, batch_size, file_type, destination_folder):
    current_batch.append(file_path)

    if len(current_batch) >= batch_size:
        save_paths_to_file(current_batch, file_type, process_file.batch_count, destination_folder)
        current_batch.clear()
        process_file.batch_count += 1

def group_and_save_files(directory, file_types, destination_folder, batch_size=10000):
    with ThreadPoolExecutor() as executor:
        for file_type, extensions in file_types.items():
            process_file.batch_count = 1
            current_batch = []
            file_generator = generate_file_paths(directory, extensions)

            for file_path in tqdm(file_generator):
                executor.submit(process_file, file_path, current_batch, batch_size, file_type, destination_folder)

            # Save any remaining files
            if current_batch:
                save_paths_to_file(current_batch, file_type, process_file.batch_count, destination_folder)


file_types = {
    'text': ['.txt', '.csv'],
    'image': ['.jpg', '.png', '.gif'],
    'document': ['.doc', '.docx', '.pdf'] }

if __name__ == "__main__":
    directory_to_search = "F:/CODES/ORCA/ComputerVison/Datasets"
    destination_folder = 'C:/Users/bolou/Pictures/Dest'

    parser = argparse.ArgumentParser()
    parser.add_argument("--directory_to_search", help="source_folder", default=directory_to_search)
    parser.add_argument("--destination_folder", help="destination_folder", default=destination_folder)
    parser.add_argument("--type_file", help="type of file", default=file_types)
    parser.add_argument("--batch_size", help="Size of each txt file", default=10000)
    args = parser.parse_args()

    print(f"Please be patient, your {args.type_file} files are being copied to {args.destination_folder}")

    group_and_save_files(args.directory_to_search, args.type_file, args.destination_folder,args.batch_size)