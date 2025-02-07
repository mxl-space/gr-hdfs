#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2025 MXL.
#
# SPDX-License-Identifier: GPL-3.0-or-later
#


import numpy as np
from gnuradio import gr
import requests
import threading
import queue
import os

class HDFSSource(gr.sync_block):
    """
    HDFSSource block reads binary data streams from an HDFS file via the WebHDFS API.
    Mimics GNU Radio's File Source block but retrieves data from HDFS instead of a local file system.
    """

    def __init__(self, filename, folder, webhdfs_addr, user="hadoop", input_type="complex", chunk_size=134217728):
        """
        Args:
            filename (str): Name of the file to read from HDFS.
            folder (str): HDFS folder path (e.g., "/user/hadoop/input/").
            webhdfs_addr (str): WebHDFS API address (e.g., "192.168.10.20:9870").
            user (str): HDFS username to use for API requests (default is "hadoop").
            input_type (str): Output data type (e.g., "complex").
            chunk_size (int): Size of the chunks to read from HDFS in bytes (default is 128 MB).
        """

        # Map input types to numpy dtypes
        input_type_dict = {
            "complex": np.complex64,
            "float": np.float32,
            "int": np.int32,
            "short": np.int16,
            "byte": np.int8
        }

        # Define out_sig based on input_type
        out_sig = [input_type_dict[input_type]]

        # Call the parent constructor
        super(HDFSSource, self).__init__(
            name="HDFSSource",
            in_sig=[],       # No input signal
            out_sig=out_sig  # Pass the determined output signal type
        )

        self.filename = filename
        self.folder = folder
        self.webhdfs_addr = webhdfs_addr
        self.user = user

        # Construct the full HDFS file path
        self.hdfs_file_path = os.path.join(self.folder, self.filename).replace("\\", "/")

        # Set the WebHDFS endpoint
        self.base_url = f"http://{self.webhdfs_addr}/webhdfs/v1{self.hdfs_file_path}?user.name={self.user}"

        # Internal buffering setup
        self.chunk_size = chunk_size
        self.data_queue = queue.Queue()
        self.stop_event = threading.Event()
        self.reader_thread = threading.Thread(target=self._reader)
        self.lock = threading.Lock()

    def start(self):
        """Prepare for reading data from HDFS."""
        print("Starting HDFSSource block...")
        try:
            # Check if the file exists
            response = requests.get(f"{self.base_url}&op=GETFILESTATUS", timeout=10)

            if response.status_code != 200:
                raise RuntimeError(f"HDFS file not found: {response.text}")
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Error initializing HDFS Source: {str(e)}")

        # Start the reader thread
        self.reader_thread.start()
        print("HDFSSource block successfully started.")
        return super().start()

    def work(self, input_items, output_items):
        """Provide data from the internal queue to the output buffer."""
        out0 = output_items[0]
        items_written = 0

        while items_written < len(out0):
            try:
                chunk = self.data_queue.get(timeout=1)  # Wait for data or timeout

                # Determine how much data to copy to the output buffer
                bytes_to_copy = min(len(chunk), (len(out0) - items_written) * out0.itemsize)
                np_chunk = np.frombuffer(chunk[:bytes_to_copy], dtype=out0.dtype)
                out0[items_written:items_written + len(np_chunk)] = np_chunk
                items_written += len(np_chunk)

                # If there's leftover data, requeue it
                if bytes_to_copy < len(chunk):
                    self.data_queue.put(chunk[bytes_to_copy:])

            except queue.Empty:
                break  # No data available in the queue

        return items_written

    def _reader(self):
        """Background thread for reading data from HDFS."""
        offset = 0

        while not self.stop_event.is_set():
            try:
                response = requests.get(
                    f"{self.base_url}&op=OPEN&offset={offset}&length={self.chunk_size}",
                    timeout=10
                )

                if response.status_code == 200:
                    data = response.content

                    if data:
                        self.data_queue.put(data)
                        offset += len(data)
                    else:
                        print("End of HDFS file reached.")
                        break
                else:
                    print(f"Failed to read from HDFS: {response.text}")
                    break

            except requests.exceptions.RequestException as e:
                print(f"Error reading from HDFS: {str(e)}")
                break

    def stop(self):
        """Clean up resources when stopping the block."""
        print("Stopping HDFSSource block...")

        # Signal the reader thread to stop
        self.stop_event.set()
        self.reader_thread.join()
        print("HDFSSource block stopped successfully.")
        return super().stop()

