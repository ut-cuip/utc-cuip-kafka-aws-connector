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

import boto3
import yaml
from confluent_kafka import Consumer
from pip._vendor.colorama import Fore


def consume():
    """
    A worker for consuming data from Kafka

    """


def main(num_workers, kafka_config):
    """
    The main process loop
    Args:
        num_workers (int): The number of workers to spin off
            DEFAULT: os.cpu_count(); the number of CPUs your system has (including threads)
    """
    print(
        Fore.MAGENTA
        + "Running with {} worker{}".format(
            num_workers, "" if num_workers == 1 else "s"
        )
        + Fore.RESET
    )
    s3client = boto3.client("s3")
    consumer = Consumer(
        {
            "bootstrap.servers": kafka_config["bootstrap-servers"],
            "group.id": kafka_config["group-id"],
            "auto.offset.reset": "beginning",
        }
    )
    consumer.subscribe(kafka_config["topics"])
    last_day_per_topic = {
        "cuip_vision_events": -1,
        "MLK_CENTRAL_AIR_QUALITY": -1,
        "MLK_DOUGLAS_AIR_QUALITY": -1,
        "MLK_GEORGIA_AIR_QUALITY": -1,
        "MLK_HOUSTON_AIR_QUALITY": -1,
        "MLK_LINDSAY_AIR_QUALITY": -1,
        "MLK_MAGNOLIA_AIR_QUALITY": -1,
        "MLK_PEEPLES_AIR_QUALITY": -1,
    }
    while True:
        try:
            msg = consumer.poll()
            if not msg or msg.error():
                continue
            topic = msg.topic()
            msg = json.loads(msg.value())
            msg_timestamp = datetime.datetime.fromtimestamp(msg["timestamp"] / 1000)
            if last_day_per_topic[topic] == -1:
                last_day_per_topic = msg_timestamp.day
            elif msg_timestamp.day != last_day_per_topic[topic]:
                """Do the S3 thing for that topic, day, month, and year for that"""
                last_day_per_topic[topic] = msg_timestamp.day
            if topic != "cuip_vision_events":
                print(topic, ":", json.dumps(msg))
            del topic, msg
            # Sleep *at all* just to save some CPU clock cycles,
            time.sleep(1 / 1000)
        except KeyboardInterrupt:
            print(Fore.RED + "Quitting.." + Fore.RESET)
            break

    consumer.close()


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
