import csv
import json
from datetime import datetime
import pytz
import zmq
from cuid2 import cuid_wrapper

cuid_generator = cuid_wrapper()

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

while True:
    message = socket.recv()
    message_data = json.loads(message.decode())

    json_string = message_data["json_string"]
    client_timezone_str = message_data["client_timezone"]

    client_timezone = pytz.timezone(client_timezone_str)

    csv_file_path = f"{cuid_generator()}.csv"

    with open(csv_file_path, "w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(
            ["time_slot", "start_date", "start_time", "duration", "timezone"]
        )

        for time_slot, entries in enumerate(json_string, start=1):
            for entry in entries:
                original_timezone = pytz.timezone(entry["timezone"])
                start_datetime = datetime.strptime(
                    f"{entry['start_date']} {entry['start_time']}", "%Y-%m-%d %H:%M"
                )
                start_datetime = original_timezone.localize(start_datetime)

                converted_datetime = start_datetime.astimezone(client_timezone)
                converted_date = converted_datetime.strftime("%Y-%m-%d")
                converted_time = converted_datetime.strftime("%H:%M")

                csv_writer.writerow(
                    [
                        time_slot,
                        converted_date,
                        converted_time,
                        entry["duration"],
                        client_timezone_str,
                    ]
                )

    socket.send(csv_file_path.encode())
    print("Sent response:", csv_file_path)
