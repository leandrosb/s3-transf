#!/usr/bin/env python
import os
import sys
import threading
import boto3
from boto3.s3.transfer import TransferConfig

class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


def main():  
    s3 = boto3.client('s3')
    config = TransferConfig(multipart_threshold=1024*25, multipart_chunksize=1024*25, use_threads=True, max_concurrency=10)

    s3.upload_file('ultimo_teste.dd', 'bucket', 'ultimo_teste.dd', Callback=ProgressPercentage('ultimo_teste.dd'), Config=config)

if __name__ == "__main__":
    main()

