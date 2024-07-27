import sys

def create_sparse_file(file_name, file_size, len_to_write):
    data_to_write = b'A' * len_to_write

    # Create a new file and write the data
    with open(file_name, 'wb') as file:
        # Write data at the begining of the file
        file.write(data_to_write)
        
        # Seek to the end of the file
        file.seek(file_size - len_to_write)
        
        # Write data at the end of the file
        file.write(data_to_write)

if __name__ == "__main__":
    # File-name, File-Size, Write Length are input to this script
    if len(sys.argv) != 4:
        print("Usage: python create_sparse_file.py <file_name> <file_size> <write_length")
        sys.exit(1)

    file_name = sys.argv[1]
    file_size = int(sys.argv[2])
    write_length = int(sys.argv[3])
    create_sparse_file(file_name, file_size, write_length)