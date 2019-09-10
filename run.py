"""
    Author: Jose Stovall
    Center for Urban Informatics and Progress | CUIP
"""
import datetime
import json
import multiprocessing
import os
import sys
import time
from io import StringIO

import boto3
import pandas as pd
import yaml
from confluent_kafka import Consumer
from pip._vendor.colorama import Fore

from timed_df import TimedDataFrameCollection


def consume(kafka_config, payload_queue):
    """
    A worker for consuming data from Kafka
    Args:
        kafka_config (dict): The kafka configuration segment from config.yaml
        payload_queue (multiprocessing.Queue): The queue that transfers Kafka payloads to the main thread
    """
    consumer = Consumer(
        {
            "bootstrap.servers": kafka_config["bootstrap-servers"],
            "group.id": kafka_config["group-id"],
            "auto.offset.reset": "beginning",
        }
    )
    consumer.subscribe(kafka_config["topics"])

    while True:
        msg = consumer.poll()
        if not msg or msg.error():
            continue

        topic = msg.topic()
        msg = json.loads(msg.value())

        # Convert the dict to string so that Pandas doesn't try to turn each item in the array into a new row
        if topic == "cuip_vision_events":
            msg["locations"] = str(msg["locations"])

        # Send it off [as a tuple] into the queue
        payload_queue.put((topic, msg))


def main(num_workers, kafka_config):
    """
    The main process loop
    Args:
        num_workers (int): The number of workers to spin off
            DEFAULT: os.cpu_count(); the number of CPUs your system has (including threads)
        kafka_config (dict): The kafka configuration segment from config.yaml
    """
    print(
        Fore.MAGENTA
        + "Running with {} worker{}".format(
            num_workers, "" if num_workers == 1 else "s"
        )
        + Fore.RESET
    )

    s3client = boto3.client("s3")

    last_prune_time = time.time()
    collection_per_topic = {
        "cuip_vision_events": TimedDataFrameCollection(60),
        "MLK_CENTRAL_AIR_QUALITY": TimedDataFrameCollection(60),
        "MLK_DOUGLAS_AIR_QUALITY": TimedDataFrameCollection(60),
        "MLK_GEORGIA_AIR_QUALITY": TimedDataFrameCollection(60),
        "MLK_HOUSTON_AIR_QUALITY": TimedDataFrameCollection(60),
        "MLK_LINDSAY_AIR_QUALITY": TimedDataFrameCollection(60),
        "MLK_MAGNOLIA_AIR_QUALITY": TimedDataFrameCollection(60),
        "MLK_PEEPLES_AIR_QUALITY": TimedDataFrameCollection(60),
    }

    # Multiprocessing work
    multiprocessing.set_start_method("spawn", force=True)
    payload_queue = multiprocessing.Queue(256)

    # Instantiate the processes as daemons since they shouldn't exist without the main process
    workers = [
        multiprocessing.Process(
            target=consume, daemon=True, args=(kafka_config, payload_queue)
        )
        for i in range(num_workers)
    ]

    for worker in workers:
        worker.start()

    while True:
        try:
            topic, msg = payload_queue.get()
            msg_timestamp = datetime.datetime.fromtimestamp(msg["timestamp"] / 1000)

            data_slice = pd.DataFrame(msg, index=[0])
            data_slice.insert(
                len(list(msg.keys())), "timestamp-iso", data_slice["timestamp"], True
            )
            data_slice["timestamp-iso"] = pd.to_datetime(
                data_slice["timestamp-iso"], unit="ms"
            )

            collection_per_topic[topic].append(data_slice)

            # Only prune data every three minutes
            if time.time() - last_prune_time > 30:
                for topic_key in collection_per_topic:
                    to_submit = collection_per_topic[topic_key].prune()
                    # If there's any data to upload to S3, do it.
                    if to_submit:
                        # Video Events and AQ events must be handled separately
                        if topic_key == "cuip_vision_events":
                            # Iterate through every one of the dataframes that was too old
                            for date_str, whole_df in to_submit:
                                cam_ids = whole_df.camera_id.unique().tolist()
                                year, month = date_str.split("-")
                                # Split out the dataframe for that day into each camera id
                                for cam_id in cam_ids:
                                    df = whole_df.query(
                                        "camera_id == '{}'".format(cam_id)
                                    )
                                    csv_buffer = StringIO()
                                    df.to_csv(csv_buffer)
                                    s3client.put_object(
                                        Bucket="utc-cuip-video-events",
                                        Key="{}/{}/{}/{}-{}-{}.csv".format(
                                            cam_id, year, month, cam_id, year, month
                                        ),
                                        Body=csv_buffer,
                                    )
                                    print(
                                        Fore.GREEN
                                        + "Uploaded {}/{}/{}/{}-{}-{}.csv to S3".format(
                                            cam_id, year, month, cam_id, year, month
                                        )
                                    )
                                    del csv_buffer, df
                                del year, month
                        else:
                            # Iterate through every one of the dataframes that was too old
                            for date_str, whole_df in to_submit:
                                nicenames = whole_df.nicename.unique().tolist()
                                year, month = date_str.split("-")
                                # Split out the dataframe for that day into each camera id
                                for nicename in nicenames:
                                    df = whole_df.query(
                                        "nicename == '{}'".format(nicename)
                                    )
                                    csv_buffer = StringIO()
                                    df.to_csv(csv_buffer)
                                    s3client.put_object(
                                        Bucket="utc-cuip-air-quality",
                                        Key="{}/{}/{}/{}-{}-{}.csv".format(
                                            nicename, year, month, nicename, year, month
                                        ),
                                        Body=csv_buffer,
                                    )
                                    print(
                                        Fore.GREEN
                                        + "Uploaded {}/{}/{}/{}-{}-{}.csv to S3".format(
                                            nicename, year, month, nicename, year, month
                                        )
                                    )
                                    del csv_buffer, df
                                del year, month
                last_prune_time = time.time()

            del topic, msg, msg_timestamp
        except KeyboardInterrupt:
            print(Fore.RED + "Quitting.." + Fore.RESET)
            for worker in workers:
                worker.terminate()
            break


if __name__ == "__main__":
    """
    Initializes the main() process
    Uses os.cpu_count() to determine default worker count if none specified
    Prints very verbosely to let even the least experienced users figure this one out
    Also colors, because pip._vendor.colorama.Fore is easy and fun to use
    """

    def print_formatting_error():
        """
        Internal function for __main__ to print formatting issues with the cmdline args
        """
        print(
            Fore.RED
            + "--num-workers / -n argument is in wrong format. It should be:"
            + Fore.RESET
        )
        print(Fore.CYAN + "   \u2022 -n=<#>" + Fore.RESET)
        print(Fore.CYAN + "   \u2022 --num-workers=<#>" + Fore.RESET)
        print(
            Fore.CYAN
            + "    where <#> represents the number of workers to spin off"
            + Fore.RESET
        )

    worker_count = os.cpu_count()
    if worker_count == None:
        worker_count = 1

    args = {
        "--help, -h": "Prints this display",
        "--num-workers, -n": "The number of workers to process with; defaults to your machine's thread count if not described (or 1 if that's not detectable)",
    }

    # Manual arg-parsing for more detailed issue reporting
    #   (and ease, wtf is wrong with the argparse lib)
    for arg in sys.argv:
        if "?" in arg or "-h" in arg:
            for arg_help in args:
                print(Fore.YELLOW + arg_help + Fore.RESET, args[arg_help])
            break
        elif "-n" in arg:
            parts = arg.split("=")
            if len(parts) != 2:
                print_formatting_error()
                print(Fore.RED + "It seems you don't have a '='" + Fore.RESET)
                exit()
            try:
                worker_count = int(parts[1])
            except ValueError:
                print_formatting_error()
                print(Fore.RED + "It seems you don't have a number after the '='")
                exit()

    print(
        Fore.BLUE
        + "This program is written to use boto3 with the expectation that AWS Auth is stored in the system's environment."
        + Fore.RESET
    )
    if not "AWS_ACCESS_KEY_ID" in os.environ:
        print(
            Fore.RED + "AWS_ACCESS_KEY_ID not found in your environment." + Fore.RESET
        )
        print(
            "Add it by using the command: "
            + Fore.YELLOW
            + 'export AWS_ACCESS_KEY_ID="<your-client-key>"'
            + Fore.RESET
        )
        exit()

    if not "AWS_SECRET_ACCESS_KEY" in os.environ:
        print(
            Fore.RED
            + "AWS_SECRET_ACCESS_KEY not found in your environment."
            + Fore.RESET
        )
        print(
            "Add it by using the command: "
            + Fore.YELLOW
            + 'export AWS_SECRET_ACCESS_KEY="<your-secret-access-key>"'
            + Fore.RESET
        )
        exit()

    print(
        Fore.BLUE
        + "Found everything needed for AWS Auth in the environment!"
        + Fore.RESET
    )

    # Reach out to the config.yaml file for the Kafka config
    if os.path.exists("./config.yaml"):
        with open("./config.yaml") as yaml_file:
            config = yaml.load(yaml_file.read(), Loader=yaml.Loader)
    else:
        print(Fore.RED + "No config.yaml found for Kafka Info" + Fore.RESET)
        exit()
    main(worker_count, config["kafka"][0])
