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
import os
import threading
import queue

class HDFSSink(gr.sync_block):
    """
    HDFSSink block writes binary data streams to an HDFS folder via WebHDFS API.
    Mimics GNU Radio's File Sink block but targets HDFS instead of a local file system.
    """

    def __init__(self, filename, folder, webhdfs_addr, user="hadoop", append="Append", input_type="complex", buffer_size=134217728):
        """
        Args:
            filename (str): Name of the file to save in HDFS.
            folder (str): HDFS folder path (e.g., "/user/mxl/input/").
            webhdfs_addr (str): WebHDFS API address (e.g., "192.168.10.20:9870").
            user (str): HDFS username to use for API requests (default is "hadoop").
            append (str): Either "Append" or "Overwrite".
            input_type (str): Input data type (e.g., "complex").
            buffer_size (int): Size of the internal buffer in bytes (default is 1 MB).
        """

        # Map input types to numpy dtypes
        input_type_dict = {
            "complex": np.complex64,
            "float": np.float32,
            "int": np.int32,
            "short": np.int16,
            "byte": np.int8
        }

        # Define in_sig based on input_type
        in_sig = [input_type_dict[input_type]]

        # Call the parent constructor
        super(HDFSSink, self).__init__(
            name="HDFSSink",
            in_sig=in_sig,   # Pass the determined input signal type
            out_sig=[]       # No output signal
        )

        self.filename = filename
        self.folder = folder
        self.webhdfs_addr = webhdfs_addr
        self.user = user
        self.append = append == "Append"

        # Construct the full HDFS file path
        self.hdfs_file_path = os.path.join(self.folder, self.filename).replace("\\", "/")

        # Set the WebHDFS endpoint
        self.base_url = f"http://{self.webhdfs_addr}/webhdfs/v1{self.hdfs_file_path}?user.name={self.user}"

        # Internal buffering setup
        self.buffer_size = buffer_size
        self.internal_buffer = bytearray()
        self.queue = queue.Queue()
        self.stop_event = threading.Event()
        self.writer_thread = threading.Thread(target=self._writer)
        self.lock = threading.Lock()

    def start(self):
        """Prepare for writing data to HDFS."""
        print("Starting HDFSSink block...")
        try:
            # Check if the file exists
            response = requests.get(f"{self.base_url}&op=GETFILESTATUS", timeout=10)

            if response.status_code == 200:
                print("File exists.")
                if not self.append:
                    print("Overwrite mode: Deleting existing file.")
                    delete_response = requests.delete(f"{self.base_url}&op=DELETE&recursive=true", timeout=10)
                    if delete_response.status_code not in [200, 201]:
                        raise RuntimeError(f"Failed to delete existing file: {delete_response.text}")
                    print("File deleted successfully.")
                    print("Creating new file.")
                    response = requests.put(f"{self.base_url}&op=CREATE&overwrite=true", timeout=10)
                    if response.status_code not in [200, 201]:
                        raise RuntimeError(f"Failed to create HDFS file: {response.text}")
                    else:
                        print("File created successfully.")
            else:
                print("File does not exist. Creating it...")
                response = requests.put(f"{self.base_url}&op=CREATE&overwrite=true", timeout=10)
                if response.status_code not in [200, 201]:
                    raise RuntimeError(f"Failed to create HDFS file: {response.text}")
                else:
                    print("File created successfully.")
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Error initializing HDFS Sink: {str(e)}")

        # Start the writer thread
        self.writer_thread.start()
        print("HDFSSink block successfully started.")
        return super().start()

    def work(self, input_items, output_items):
        """Push input data to the internal buffer."""
        in0 = input_items[0]

        with self.lock:
            self.internal_buffer.extend(in0.tobytes())

        # Flush the buffer to the queue if it exceeds the threshold
        if len(self.internal_buffer) >= self.buffer_size:
            self.queue.put(self.internal_buffer[:self.buffer_size])
            self.internal_buffer = self.internal_buffer[self.buffer_size:]

        return len(in0)

    def _writer(self):
        """Background thread for writing data to HDFS."""
        while not self.stop_event.is_set() or not self.queue.empty():
            try:
                chunk = self.queue.get(timeout=1)  # Wait for data or timeout
                response = requests.post(
                    f"{self.base_url}&op=APPEND",  # Always use APPEND since file handled in start()
                    headers={"Content-Type": "application/octet-stream"},
                    data=chunk,
                    timeout=10
                )
                if response.status_code not in [200, 201]:
                    print(f"Failed to write to HDFS: {response.text}")
            except queue.Empty:
                continue  # No data in the queue, keep waiting
            except requests.exceptions.RequestException as e:
                print(f"Error writing to HDFS: {str(e)}")

    def stop(self):
        """Clean up resources when stopping the block."""
        print("Stopping HDFSSink block...")

        # Flush any remaining data in the internal buffer to the queue
        with self.lock:
            if self.internal_buffer:
                self.queue.put(self.internal_buffer)
                self.internal_buffer = bytearray()

        # Signal the writer thread to stop
        self.stop_event.set()
        self.writer_thread.join()
        print("HDFSSink block stopped successfully.")
        return super().stop()

