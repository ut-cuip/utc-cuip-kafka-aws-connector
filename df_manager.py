"""
    Author: Jose Stovall
    Center for Urban Informatics and Progress | CUIP
"""
import calendar
import datetime
import glob
import os
import time
from io import StringIO

import pandas as pd
import s3fs
from pip._vendor.colorama import Fore


class DataframeManager:
    def __init__(self, topic: str) -> None:
        """
        Manages dataframes for a given topic
        Args:
            topic (str): The topic name that this manager is responsible for
            flush_intval (int): How regularly (in seconds) to flush the data to AWS and disk
        """
        self.topic = topic
        self.records = []
        self.fs = s3fs.S3FileSystem(anon=False)
        if not os.path.exists("./cache"):
            os.mkdir("./cache")

    def append(self, msg: dict) -> None:
        """
        Keeps track of messages as they come in.
        """
        # Older data my be missing the timestamp; account for this:
        if not "timestamp" in msg:
            return
        self.records.append(msg)

    def flush(self) -> None:
        """
        Writes data out to disk, uploads old CSVs then deletes them
        """
        print(
            "Topic {} accumulated {} messages".format(
                Fore.GREEN + self.topic + Fore.RESET,
                Fore.CYAN + str(len(self.records)) + Fore.RESET,
            )
        )
        if len(self.records) <= 0:
            return

        # This is a slice of the random-dated data - we need to split it up by month
        df_slice = pd.DataFrame.from_records(self.records)
        df_slice.insert(
            len(df_slice.columns), "timestamp-iso", df_slice["timestamp"], True
        )
        df_slice["timestamp-iso"] = pd.to_datetime(df_slice["timestamp-iso"], unit="ms")

        unique_months = (
            df_slice["timestamp-iso"].dt.strftime("%Y-%m").drop_duplicates().tolist()
        )

        for unique_month in unique_months:
            year, month = unique_month.split("-")
            year, month = int(year), int(month)
            if year == 1970:
                continue
            by_month = df_slice[
                (df_slice["timestamp-iso"] > "{}-01".format(unique_month))
                & (
                    df_slice["timestamp-iso"]
                    < "{}-{}".format(unique_month, calendar.monthrange(year, month)[1])
                )
            ]
            if self.topic == "cuip_vision_events":
                camera_ids = df_slice["camera_id"].unique().tolist()
                # Write to AWS
                for cam_id in camera_ids:
                    submit_df = by_month.query("camera_id == '{}'".format(cam_id))
                    local_filename = "./cache/{}-{}-{}.csv".format(cam_id, year, month)
                    s3_filename = "utc-cuip-video-events/{}/year={}/month={}/{}-{}-{}.csv".format(
                        cam_id, year, month, cam_id, year, month
                    )
                    # Check to see if the df already exists:
                    if os.path.exists(local_filename):
                        df = pd.read_csv(local_filename)
                    else:
                        df = pd.DataFrame()
                    df = df.append(submit_df, ignore_index=True, sort=False)
                    df.to_csv("s3://{}".format(s3_filename))
                    df.to_csv(local_filename)
                    del df, submit_df
            elif "AIR_QUALITY" in self.topic:
                nicenames = df_slice["nicename"].unique().tolist()
                for nicename in nicenames:
                    submit_df = by_month.query("nicename == '{}'".format(nicename))
                    local_filename = "./cache/{}-{}-{}.csv".format(
                        nicename, year, month
                    )
                    s3_filename = "utc-cuip-air-quality/{}/year={}/month={}/{}-{}-{}.csv".format(
                        nicename, year, month, nicename, year, month
                    )
                    if "nan" in s3_filename:
                        continue
                    if os.path.exists(local_filename):
                        df = pd.read_csv(local_filename)
                    else:
                        df = pd.DataFrame()
                    df = df.append(submit_df, ignore_index=True, sort=False)
                    df.to_csv("s3://{}".format(s3_filename))
                    df.to_csv(local_filename)
                    del df, submit_df

            else:
                print(
                    Fore.RED
                    + self.topic
                    + " is not a recogniezd topic name. Please manually configure this."
                    + Fore.RESET
                )

        del self.records[:]
        self.records = []
        print("Done flushing for topic {}".format(self.topic))
