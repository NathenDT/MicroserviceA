import csv
import json
import zmq
import os
from cuid2 import cuid_wrapper

cuid_generator = cuid_wrapper()

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

while True:
    message = socket.recv()
    message_data = json.loads(message.decode())

    json_string = message_data["json_string"]
    output_dir = message_data["output_dir"]

    csv_file_path = os.path.join(output_dir, f"{cuid_generator()}.csv")

    with open(csv_file_path, "w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(
            ["time_slot", "start_date", "start_time", "duration", "timezone"]
        )

        for time_slot, entries in enumerate(json_string, start=1):
            for entry in entries:
                csv_writer.writerow(
                    [
                        time_slot,
                        entry["start_date"],
                        entry["start_time"],
                        entry["duration"],
                        entry["timezone"],
                    ]
                )

    socket.send(csv_file_path.encode())
    print("Sent response:", csv_file_path)
