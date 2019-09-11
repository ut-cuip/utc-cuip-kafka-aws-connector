import datetime
import glob
import os
import time
from io import StringIO

import boto3
import pandas as pd


class DataframeManager:
    def __init__(self, topic: str) -> None:
        """
        Manages dataframes for a given topic
        Args:
            topic (str): The topic name that this manager is responsible for
        """
        self.df_map = {}
        self.topic = topic
        self.s3client = boto3.client("s3")
        self.last_flush_time = time.time()
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
        if not self.df_map[msg_date]:
            cache_name = "{} {}.csv".format(self.topic, msg_date)
            if not os.path.exists("./cache/{}".format(cache_name)):
                self.df_map[msg_date] = pd.DataFrame()
            # Otherwise just read it in
            else:
                self.df_map[msg_date] = pd.read_csv("./cache/{}".format(cache_name))

        # Slap it in there!
        self.df_map[msg_date] = self.df_map[msg_date].append(data_slice)

        del data_slice, msg_date, msg_timestamp, cache_name

        # Flush every ... 10 minutes?
        if time.time() - self.last_flush_time >= 600:
            self.flush()

    def flush(self) -> None:
        """
        Writes data out to disk, uploads old CSVs then deletes them
        """
        for filename in glob.glob("./cache/{}*.csv".format(self.topic)):
            df = pd.read_csv(filename)
            year, month = filename.replace("{} ".format(self.topic))
            self.submit(df, year, month)
            del df, year, month
            os.remove(filename)

        for date in self.df_map:
            self.df_map[date].to_csv("./cache/{} {}.csv".format(self.topic, date))

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
                csv_buffer = StringIO()
                df.to_csv(csv_buffer)
                self.s3client.put_object(
                    Bucket="utc-cuip-video-events",
                    Key="utc-cuip-video-events/{}/{}/{}/{}-{}-{}.csv".format(
                        camera_id, year, month, camera_id, year, month
                    ),
                    Body=csv_buffer.getvalue(),
                )
        else:
            csv_buffer = StringIO()
            dataframe.to_csv(csv_buffer)
            self.s3client.put_object(
                Bucket="utc-cuip-air-quality",
                Key="utc-cuip-air-quality/{}/{}/{}/{}-{}-{}.csv".format(
                    self.topic.lower(), year, month, self.topic.lower(), year, month
                ),
                Body=csv_buffer.getvalue(),
            )