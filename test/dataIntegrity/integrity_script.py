import sys
import io
import random

# Class to hold the test parameters
class TestParams:
    def __init__(self, local_path, remote_path, file_size, write_length):
        self.local_path = local_path
        self.remote_path = remote_path
        self.file_size = file_size
        self.write_length = write_length

def read_all_data(file_path):
    with open(file_path, 'rb') as file:
        data = file.read()
    
    print("Data Read : ", file_path, " : " ,str(len(data)))
    return data, len(data)

# Method to create a sparse file with hole in middle
def test_create_sparse(params):
    data_to_write = random.randbytes(params.write_length)

    local_file_name = params.local_path + "/sparse_" + str(params.file_size)
    remote_file_name = params.remote_path + "/sparse_" + str(params.file_size) 

    # Create a new file and write the data
    with open(remote_file_name, 'wb') as remote_file, open(local_file_name, 'wb') as local_file :
        # Write data at the start of the file
        remote_file.write(data_to_write)
        local_file.write(data_to_write)
        
        # Seek to the end of the file
        remote_file.seek(params.file_size - params.write_length)
        local_file.seek(params.file_size - params.write_length)
        
        # Write data at the end of the file
        remote_file.write(data_to_write)
        local_file.write(data_to_write)


# Method to create a sparse file with hole in the begining
def test_create_front_hole(params):
    data_to_write = random.randbytes(params.write_length)

    local_file_name = params.local_path + "/front_hole_" + str(params.file_size) 
    remote_file_name = params.remote_path + "/front_hole_" + str(params.file_size)

    # Create a new file and write the data
    with open(remote_file_name, 'wb') as remote_file, open(local_file_name, 'wb') as local_file :
        # Seek to the end of the file
        remote_file.seek(params.file_size - params.write_length)
        local_file.seek(params.file_size - params.write_length)
        
        # Write data at the end of the file
        remote_file.write(data_to_write)
        local_file.write(data_to_write)

def test_read_data(params):
    local_file_name = params.local_path + "/test_data_" + str(params.file_size)
    remote_file_name = params.remote_path + "/test_data_" + str(params.file_size) 

    remote_data, remote_num_bytes = read_all_data(remote_file_name)
    local_data, local_num_bytes = read_all_data(local_file_name)

    if remote_data == local_data and remote_num_bytes == local_num_bytes and remote_num_bytes == params.file_size:
        print("Data Integrity Test Passed")
    else:
        print("Data Integrity Test Failed")


def test_write_data(params):
    data_to_write = random.randbytes(params.write_length)

    local_file_name = params.local_path + "/test_data_" + str(params.file_size)
    remote_file_name = params.remote_path + "/test_data_" + str(params.file_size) 

    written = 0
    # Create a new file and write the data
    with open(remote_file_name, 'wb') as remote_file, open(local_file_name, 'wb') as local_file :
        while written < params.file_size:
            to_write = min(params.write_length, params.file_size - written)
            # Write data at the start of the file
            remote_file.write(data_to_write[:to_write])
            local_file.write(data_to_write[:to_write])

            written += params.write_length
       
# Main method to simulate all test cases
if __name__ == "__main__":
    # File-name, File-Size, Write Length are input to this script
    if len(sys.argv) != 6:
        print("Usage: python integrity_script.py <test_case> <local_path> <remote_path> <file_size> <write_length")
        sys.exit(1)

    test_case = sys.argv[1]
    local_path = sys.argv[2]
    remote_path = sys.argv[3]
    file_size = int(sys.argv[4])
    write_length = int(sys.argv[5])

    # Create an instance of FileParams
    params = TestParams(local_path, remote_path, file_size, write_length)

    function_name = f"test_{test_case}"
    if function_name in globals():
        globals()[function_name](params)
    else:
        print("Invalid Test Case.")
        sys.exit(1)