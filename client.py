import json
import zmq

JSON_FILE_PATH = "input.json"
OUTPUT_DIR = "outputs"

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:5555")

with open(JSON_FILE_PATH, "r", encoding="utf-8") as json_file:
    data = json.load(json_file)

message_data = {"json_string": data, "output_dir": OUTPUT_DIR}
socket.send(json.dumps(message_data).encode())

csv_file_path = socket.recv().decode()

print("Received CSV file path:", csv_file_path)
