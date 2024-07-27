import sys

# Class to hold the test parameters
class TestParams:
    def __init__(self, file_name, file_size, write_length):
        self.file_name = file_name
        self.file_size = file_size
        self.write_length = write_length

# Method to create a sparse file
def test_create_sparse(params):
    data_to_write = b'A' * params.write_length

    # Create a new file and write the data
    with open(params.file_name, 'wb') as file:
        # Write data at the start of the file
        file.write(data_to_write)
        
        # Seek to the end of the file
        file.seek(params.file_size - params.write_length)
        
        # Write data at the end of the file
        file.write(data_to_write)


# Main method to simulate all test cases
if __name__ == "__main__":
    # File-name, File-Size, Write Length are input to this script
    if len(sys.argv) != 5:
        print("Usage: python create_sparse_file.py <test_case> <file_name> <file_size> <write_length")
        sys.exit(1)

    test_case = sys.argv[1]
    file_name = sys.argv[2]
    file_size = int(sys.argv[3])
    write_length = int(sys.argv[4])

    # Create an instance of FileParams
    params = TestParams(file_name, file_size, write_length)

    function_name = f"test_create_{test_case}"
    if function_name in globals():
        globals()[function_name](params)
    else:
        print("Invalid Test Case.")
        sys.exit(1)