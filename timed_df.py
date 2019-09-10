"""
    Author: Jose Stovall
    Center for Urban Informatics and Progress | CUIP
"""

import datetime
import time

import pandas as pd


class TimedDataFrameCollection:
    def __init__(self, age_threshold: int) -> None:
        """
        A self-contained class for managing dataframes with last_modified associations
        Args:
            age_threshold (int):
                the maximum age (in seconds) that a dataframe can go without modification before it's considered "old"
        """
        self.frame_storage = {}
        self.age_threshold = age_threshold

    def append(self, data_slice: pd.DataFrame) -> None:
        """
        Appends a new data slice to the current collection
        Args:
            data_slice (pandas.DataFrame): the dataframe slice to modify
        """
        slice_ts = datetime.datetime.fromtimestamp(data_slice["timestamp"] / 1000)
        format_str = "{}-{}".format(slice_ts.year, slice_ts.month)
        if not format_str in self.frame_storage:
            self.frame_storage[format_str] = {
                "dataframe": data_slice,
                "last_modified": time.time(),
            }
        else:
            self.frame_storage[format_str]["dataframe"] = self.frame_storage[
                format_str
            ]["dataframe"].append(data_slice, ignore_index=True, sort=False)
            self.frame_storage[format_str]["last_modified"] = time.time()

    def prune(self) -> list:
        """
        Filters through the current dict of frames to determine which ones are out of date
        Returns:
            (list): List of tuples in form (date, dataframe) that have not been modified since @self.age_threshold 
        """
        collected_tuples = list()

        for month in self.frame_storage:
            if (
                time.time() - self.frame_storage[month]["last_modified"]
                > self.age_threshold
            ):
                collected_tuples.append(
                    (month, (self.frame_storage[month]["dataframe"]))
                )
                del self.frame_storage[month]

        return collected_tuples
