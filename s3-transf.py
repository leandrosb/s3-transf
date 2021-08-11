#!/usr/bin/env python
from genericpath import isdir
import os
import sys
import argparse
import time
import logging
from fnmatch import fnmatch
import threading
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError



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

def uploads3(file_name, bucket, object_name=None):
    s3 = boto3.client('s3')
    config = TransferConfig(multipart_threshold=1024*25, 
                            multipart_chunksize=1024*25, 
                            use_threads=True, max_concurrency=10)
    try:
        responde = s3.upload_file(file_name, bucket, object_name, 
                                  Callback=ProgressPercentage(file_name), 
                                  Config=config)
                                
    except ClientError as e:
        logging.error(e)
        return False
    return True

<<<<<<< HEAD
    s3.upload_file('ultimo_teste.dd', 'bucket', 'ultimo_teste.dd', Callback=ProgressPercentage('ultimo_teste.dd'), Config=config)
=======
def get_args():
    parser = argparse.ArgumentParser(
        description='Argumentos para backup S3')
>>>>>>> 46e193feb84d5ec3065ef7e1ab449be2bb068fb1


    parser.add_argument('--path','-p',
                        required=True,
                        action='store',
                        default=None,
                        help='Path de origem de busca')
    parser.add_argument('--search','-s',
                        required=True,
                        action='store',
                        default=None,
                        help='filtragem para busca de arquivos')
    parser.add_argument('--create','-c',
                        required=False,
                        action='store',
                        type=int,
                        default=None,
                        help='Busca dos arquivos por data de criação (Formato em quantidade de dias)')
    parser.add_argument('--modify','-m',
                        required=False,
                        action='store',
                        type=int,
                        default=None,
                        help='Busca dos arquivos por data de Modificação (Formato em quantidade de dias)')

    args = parser.parse_args()
    return args

def findfile(path_search, busca, tipo_busca=None, tempo=None):
    now = time.time()
    for relpath, dirs, files in os.walk(path_search):
        for name in files:
            if fnmatch(name, busca): 
                full_path = os.path.join(path_search, relpath, name)
                if tipo_busca == 'c':
                    return_time = os.path.getctime(full_path)
                elif tipo_busca == 'm':
                    return_time = os.path.getmtime(full_path)
                else:
                    return_time = None

                if return_time is None or return_time > (now - tempo):
                    print(os.path.normpath(os.path.abspath(full_path)))


def main():
    tipo_busca = None
    args = get_args()
    if not args.create is None:
        tipo_busca = 'c'
        tempo = int(args.create) * 86400 #segundos por dia
    elif not args.modify is None:
        tipo_busca = 'm'
        tempo = int(args.modify) * 86400 #segundos por dia
    else:
        tempo = 0
        
    findfile(args.path, args.search, tipo_busca, tempo )


# Start program
if __name__ == "__main__":
   main()
