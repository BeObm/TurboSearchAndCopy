import argparse
import os
import shutil
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor

def copy_file(file_path, destination_folder):
    try:
        file_name = os.path.basename(file_path)
        destination_path = os.path.join(destination_folder, file_name)

        if os.path.exists(destination_path):
            base_name, ext = os.path.splitext(file_name)
            count = 1
            while os.path.exists(os.path.join(destination_folder, f"{base_name}_{count}{ext}")):
                count += 1
            new_file_name = f"{base_name}_{count}{ext}"
            destination_path = os.path.join(destination_folder, new_file_name)

        shutil.copy2(file_path, destination_path)
    except Exception as e:
        print(f"Error copying {file_path}: {e}")


def copy_files_from_text_files(txt_files_folder, destination_folder, batch_size=10000, max_workers=None):
    txt_files = [f for f in os.listdir(txt_files_folder) if f.endswith('.txt')]

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for txt_file in tqdm(txt_files):
            txt_file_path = os.path.join(txt_files_folder, txt_file)

            with open(txt_file_path, 'r') as file:
                file_paths = [line.strip() for line in file]

            total_files = len(file_paths)
            batch_count = 0

            for i in range(0, total_files, batch_size):
                current_batch = file_paths[i:i + batch_size]
                batch_count += 1

                futures = []
                for file_path in current_batch:
                    if os.path.exists(file_path):
                        future = executor.submit(copy_file, file_path, destination_folder)
                        futures.append(future)

                # Wait for all copying tasks in the current batch to complete
                for future in futures:
                    future.result()


if __name__ == "__main__":
    txt_files_folder = "C:/Users/bolou/Pictures/Dest"
    destination_folder = 'C:/Users/bolou/Pictures/Destination'

    parser = argparse.ArgumentParser()
    parser.add_argument("--txt_files_folder", help="source_folder", default=txt_files_folder)
    parser.add_argument("--destination_folder", help="destination_folder", default=destination_folder)
    args = parser.parse_args()

    if not os.path.exists(args.destination_folder):
        os.makedirs(args.destination_folder)
    print(f"Please be patient, your files are being copied to {args.destination_folder}")

    copy_files_from_text_files(args.txt_files_folder, args.destination_folder, max_workers=4)