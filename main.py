import os
import shutil
import argparse
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing
import time

file_types = {
    'text': ['.txt', '.csv'],
    'image': ['.jpg', '.png', '.gif'],
    'document': ['.doc', '.docx', '.pdf']
}


def copy_file(src_filepath, dest_folder,counter):
    _, filename = os.path.split(src_filepath)
    dest_filepath = os.path.join(dest_folder, filename)
    count = 1
    while os.path.exists(dest_filepath):
        name, ext = os.path.splitext(filename)
        filenam = f"{count}_{name}{ext}"
        dest_filepath = os.path.join(dest_folder, filenam)
        count += 1

    shutil.copy2(src_filepath, dest_filepath)
    with counter.get_lock():
        counter.value += 1


def copy_files_by_type(src_folder, dest_folder, file_type, file_types=file_types):
    start_time = time.time()
    counter = multiprocessing.Value('i', 0)

    futures = []

    with ProcessPoolExecutor() as process_executor, ThreadPoolExecutor() as thread_executor:
        for foldername, subfolders, filenames in tqdm(os.walk(src_folder), desc="Processing", unit="iteration"):
            for filename in filenames:
                _, file_extension = os.path.splitext(filename)
                allowed_extensions = file_types[file_type]
                if file_extension.lower() in allowed_extensions:
                    src_filepath = os.path.join(foldername, filename)
                    futures.append(thread_executor.submit(copy_file, src_filepath, dest_folder, counter))

        # Wait for all the copying tasks to complete
        tqdm(total=len(futures), desc="Copying", unit="file").update(len(futures))
        for future in tqdm(futures, desc="Copying", unit="file"):
            future.result()

    elapsed_time = time.time() - start_time
    copied_files_count = counter.value
    print(f"\nCopied {copied_files_count} {args.type_file} files in {elapsed_time:.2f} seconds.")



def copy_files_by_type2(src_folder, dest_folder, file_type, file_types=file_types, num_threads=8):
    # Create a ThreadPoolExecutor with the specified number of threads
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []

        # Iterate through the source folder and its subfolders
        for foldername, subfolders, filenames in tqdm(os.walk(src_folder), desc="Processing", unit="iteration"):
            for filename in filenames:
                _, file_extension = os.path.splitext(filename)
                allowed_extensions = file_types[file_type]
                if file_extension.lower() in allowed_extensions:
                    src_filepath = os.path.join(foldername, filename)

                    # Use ThreadPoolExecutor to submit copy_file function
                    futures.append(executor.submit(copy_file, src_filepath, dest_folder))

        # Wait for all the copying tasks to complete
        tqdm(total=len(futures), desc="Copying", unit="file").update(len(futures))
        for future in tqdm(futures, desc="Copying", unit="file"):
            future.result()
source_folder = ""
destination_folder = ""

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--source_folder", help="source_folder", default=source_folder)
    parser.add_argument("--destination_folder", help="destination_folder", default=destination_folder)
    parser.add_argument("--type_file", help="type of file", default="image")
    args = parser.parse_args()

    print(f"Please be patient, your {args.type_file} files are being copied to {args.destination_folder}")
    copy_files_by_type(args.source_folder, args.destination_folder, args.type_file)
