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
from pip._vendor.colorama import Fore

import s3fs


class DataframeManager:
    def __init__(self, topic: str, flush_intval: int) -> None:
        """
        Manages dataframes for a given topic
        Args:
            topic (str): The topic name that this manager is responsible for
            flush_intval (int): How regularly (in seconds) to flush the data to AWS and disk
        """
        self.topic = topic
        self.last_flush_time = time.time()
        self.flush_intval = flush_intval
        self.records = []
        self.fs = s3fs.S3FileSystem(anon=False)


    def append(self, msg: dict) -> None:
        """
        Keeps track of messages as they come in.
        """
        # Older data my be missing the timestamp; account for this:
        if not "timestamp" in msg:
            return
        self.records.append(msg)
        print("{} processed {} records".format(self.topic, len(self.records)))

        if time.time() - self.last_flush_time >= self.flush_intval:
            self.flush()

    def flush(self) -> None:
        """
        Writes data out to disk, uploads old CSVs then deletes them
        """
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
            by_month = df_slice[
                (df_slice["timestamp-iso"] > "{}-01".format(unique_month))
                & (
                    df_slice["timestamp-iso"]
                    < "{}-{}".format(
                        unique_month, calendar.monthrange(year, month)[1]
                    )
                )
            ]
            if self.topic == "cuip_vision_events":
                camera_ids = df_slice["camera_id"].unique().tolist()
                # Write to AWS
                for cam_id in camera_ids:
                    submit_df = by_month.query("camera_id == '{}'".format(cam_id))
                    filename = "utc-cuip-video-events/{}/year={}/month={}/{}-{}-{}.csv".format(
                        cam_id, year, month, cam_id, year, month
                    )
                    # Check to see if the df already exists:
                    if self.fs.exists(filename):
                        df = pd.read_csv("s3://{}".format(filename))
                    else:
                        df = pd.DataFrame()
                    df = df.append(submit_df, ignore_index=True, sort=False)
                    df.to_csv("s3://{}".format(filename))
                    del df, submit_df, filename
            elif "AIR_QUALITY" in self.topic:
                nicenames = df_slice["nicename"].unique().tolist()
                for nicename in nicenames:
                    submit_df = by_month.query("nicename == '{}'".format(nicename))
                    filename = "utc-cuip-air-quality/{}/year={}/month={}/{}-{}-{}.csv".format(
                        nicename, year, month, nicename, year, month
                    )
                    if "nan" in filename:
                        continue
                    if self.fs.exists(filename):
                        df = pd.read_csv("s3://{}".format(filename))
                    else:
                        df = pd.DataFrame()
                    df = df.append(submit_df, ignore_index=True, sort=False)
                    df.to_csv("s3://{}".format(filename))
                    del df, submit_df, filename

            else:
                print(
                    Fore.RED
                    + self.topic
                    + " is not a recogniezd topic name. Please manually configure this."
                    + Fore.RESET
                )


        del self.records[:]
        self.records = []
        self.last_flush_time = time.time()
