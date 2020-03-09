import os
import csv
import sys
import json
from collections import deque
from requests_html import HTMLSession
from typing import Callable, NoReturn, Dict, IO


SIGN_STDOUT = '-'
FORMAT_CSV = 'csv'
FORMAT_JL = 'jl'


def start(start_url: str, callback: Callable, out_path: str, out_format: str) -> NoReturn:
    """
    Tasks' handler
    :param start_url:
    :param callback:
    :param out_path:
    :param out_format:
    """
    start_task = (start_url, callback)
    tasks = deque([start_task])

    out_file = sys.stdout if out_path == SIGN_STDOUT else open(out_path, 'w+', buffering=1)

    try:
        while tasks:
            url, callback = tasks.popleft()
            print(url)
            session = HTMLSession()
            resp = session.get(url)

            for result in callback(resp):
                if isinstance(result, dict):
                    if out_format == FORMAT_CSV:
                        _write_csv(result, out_file, out_path)
                    elif out_format == FORMAT_JL:
                        _write_jl(result, out_file)
                    else:
                        raise NotImplementedError('The output format is not implemented.')
                else:
                    if result:
                        tasks.append(result)
    finally:
        out_file.close()


def _write_jl(row: Dict[str, str], out_file: IO[str]) -> NoReturn:
    """
    Dump data to JL file
    :param row:
    :param out_file:
    """
    json.dump(row, out_file)
    out_file.write('\n')


def _write_csv(row: Dict[str, str], out_file: IO[str], out_path: str) -> NoReturn:
    """
    Dump data into CSV file
    :param row:
    :param out_file:
    """
    fieldnames = row.keys()
    writer = csv.DictWriter(out_file, delimiter=',', fieldnames=fieldnames)
    if os.stat(out_path).st_size > 0:
        writer.writerow(row)
    else:
        writer.writeheader()
