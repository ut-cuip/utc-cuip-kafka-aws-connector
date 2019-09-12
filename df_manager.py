"""
    Author: Jose Stovall
    Center for Urban Informatics and Progress | CUIP
"""
import datetime
import glob
import os
import time
from io import StringIO

import boto3
import pandas as pd


class DataframeManager:
    def __init__(self, topic: str, flush_intval: int) -> None:
        """
        Manages dataframes for a given topic
        Args:
            topic (str): The topic name that this manager is responsible for
            flush_intval (int): How regularly (in seconds) to flush the data to AWS and disk
        """
        self.df_map = {}
        self.topic = topic
        self.s3client = boto3.client("s3")
        self.last_flush_time = time.time()
        self.flush_intval = flush_intval
        if not os.path.exists("./cache"):
            os.mkdir("./cache")

    def append(self, msg: dict) -> None:
        """
        Appends the message to the right dataframe, reading from/writing to cache when necessary
        Args:
            msg (dict): The Kafka message to append into the categorized Dataframe
        """
        msg_timestamp = datetime.datetime.fromtimestamp(msg["timestamp"] / 1000)
        msg_date = "{}-{}".format(msg_timestamp.year, msg_timestamp.month)

        # Prepare the data slice
        data_slice = pd.DataFrame(msg, index=[0])
        data_slice.insert(
            len(list(msg.keys())), "timestamp-iso", data_slice["timestamp"], True
        )
        data_slice["timestamp-iso"] = pd.to_datetime(
            data_slice["timestamp-iso"], unit="ms"
        )

        # Make sure the overarching dataframe is there
        if not msg_date in self.df_map:
            cache_name = "{} {}.csv".format(self.topic, msg_date)
            if not os.path.exists("./cache/{}".format(cache_name)):
                self.df_map[msg_date] = pd.DataFrame(index=[0])
            # Otherwise just read it in
            else:
                self.df_map[msg_date] = pd.read_csv(
                    "./cache/{}".format(cache_name), index_col=False
                )

        # Slap it in there!
        self.df_map[msg_date] = self.df_map[msg_date].append(
            data_slice, ignore_index=True, sort=False
        )

        del data_slice, msg_date, msg_timestamp

        # Flush every ... 10 minutes?
        if time.time() - self.last_flush_time >= self.flush_intval:
            self.flush()

    def flush(self) -> None:
        """
        Writes data out to disk, uploads old CSVs then deletes them
        """
        for filename in glob.glob("./cache/{}*.csv".format(self.topic)):
            df = pd.read_csv(filename, index_col=False)
            year, month = (
                filename.replace("{} ".format(self.topic), "")
                .replace(".csv", "")
                .replace("./cache/", "")
                .split("-")
            )
            self.submit(df, year, month)
            del df, year, month
            os.remove(filename)

        for date in self.df_map:
            self.df_map[date].to_csv(
                "./cache/{} {}.csv".format(self.topic, date), index=False
            )

        self.df_map.clear()
        self.last_flush_time = time.time()

    def submit(self, dataframe: pd.DataFrame, year: int, month: int) -> None:
        """
        Submits the dataframe as a CSV to S3
        Args:
            dataframe (pandas.DataFrame): The Dataframe to submit
            year (int) : the year the of the dataframe's contents (YYYY)
            month (int) : the month the of the dataframe's contents (MM)
        """
        if self.topic == "cuip_vision_events":
            camera_ids = dataframe.camera_id.unique().tolist()
            for camera_id in camera_ids:
                df = dataframe.query("camera_id == '{}'".format(camera_id))
                if df.size > 0:
                    csv_buffer = StringIO()
                    df.to_csv(csv_buffer, index=False)
                    self.s3client.put_object(
                        Bucket="utc-cuip-video-events",
                        Key="{}/{}/{}/{}-{}-{}.csv".format(
                            camera_id, year, month, camera_id, year, month
                        ),
                        Body=csv_buffer.getvalue(),
                    )
                del df
        else:
            csv_buffer = StringIO()
            dataframe.to_csv(csv_buffer, index=False)
            self.s3client.put_object(
                Bucket="utc-cuip-air-quality",
                Key="{}/{}/{}/{}-{}-{}.csv".format(
                    self.topic.lower(), year, month, self.topic.lower(), year, month
                ),
                Body=csv_buffer.getvalue(),
            )
