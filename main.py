import subprocess

import gevent
from gevent import monkey
monkey.patch_all(thread=False)
from gevent.libev.corecext import traceback
from gevent import sleep
from gevent.queue import Queue
from gevent.threading import Lock

import copy
import datetime
import math
import os
import pickle
import signal
import sys
import requests
import argparse
import logging

requests.packages.urllib3.disable_warnings()

sys.setrecursionlimit(2000)

class HttpMultiThreadDownloader:

    TOTAL_BUFFER_SIZE = 1024 * 1024 * 16
    CHUNK_SIZE = 1024
    MIN_TASK_CHUNK_SIZE = 1 * CHUNK_SIZE
    DEFAULT_THREAD_NUMBER = 32

    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36", 'Accept-Language':'zh-CN,zh;q=0.9'}

    def __init__(self, url=None, file_name=None, path_to_store=None, total_size=None, thread_number=None, logger=None, print_progress=False):

        assert url or file_name

        file_name = file_name if file_name else url.split('?')[0].split('/')[-1]
        file_name_split = file_name.split('.')
        if len(file_name_split[0]) > 64:
            file_name_split[0] = file_name_split[0][:64]
        file_name = '.'.join(file_name_split)
        self.path_to_store = path_to_store if path_to_store else f"{os.environ['HOME']}/Downloads"

        self.logger = logger or logging
        self.print_progress = print_progress
        self.url = url
        self.total_size = total_size
        self.file_name_with_path = self.path_to_store + '/' + file_name
        self.breakpoint_file_path = self.file_name_with_path + '.tmp'
        self.file_seeker = None
        self.thread_number = thread_number
        self.thread_buffer_size = None
        self.remaining_segments = []
        self.start_time = None
        self.segment_dispatch_task = None
        self.speed_calculation_task = None
        self.bytearray_of_threads = None
        self.last_progress_time = None
        self.last_downloaded_size = 0
        self.workers = []
        self.coworker_end_to_indexes_map = dict()
        self.to_dispatcher_queue = Queue()
        self.complete_notify_lock = Lock()

        self.download_info = self.get_download_info()

    def get_total_size(self):
        while True:
            try:
                with gevent.Timeout(10):
                    res = requests.get(self.url, stream=True, verify=False, headers=self.headers)
                break
            except KeyboardInterrupt:
                os.kill(os.getpid(),signal.SIGTERM)
            except (gevent.timeout.Timeout,requests.exceptions.ProxyError, requests.exceptions.ConnectionError):
                self.logger.error(traceback.format_exc())
        if int(res.status_code / 100) == 2 and 'Content-Length' in res.headers:
            return int(res.headers['Content-Length'])
        else:
            raise RuntimeError(f'Not support multi thread: {self.url}')

    def get_download_info(self):
        if os.path.exists(self.file_name_with_path) and os.path.exists(self.breakpoint_file_path):
            with open(self.breakpoint_file_path, 'rb') as f:
                info_map = pickle.load(f)
            self.file_seeker = open(self.file_name_with_path, "r+b")
            thread_number = info_map['thread_number']
            self.remaining_segments = info_map['remaining_segments']
            if self.thread_number is not None and self.thread_number > thread_number:
                self.remaining_segments += [[0,0]] * (self.thread_number - thread_number)
                for index in range(thread_number, self.thread_number):
                    self.balance_remaining_segments(index)
            else:
                self.thread_number = thread_number

            self.total_size = info_map['total_size']
            self.url = info_map['url']
        else:
            if self.total_size is None:
                self.total_size = self.get_total_size()
            info_map = {
                'url': self.url,
                'thread_number': self.thread_number,
                'total_size': self.total_size
            }
            if self.thread_number is None:
                self.thread_number = self.DEFAULT_THREAD_NUMBER
            subprocess.Popen(f'mkdir -p {self.path_to_store}', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.file_seeker = open(self.file_name_with_path, "w+b")
            if math.floor(self.total_size / self.CHUNK_SIZE) < self.thread_number:
                self.thread_number = math.floor(self.total_size / self.CHUNK_SIZE)
            divided_segment_size = math.floor(self.total_size / self.thread_number / self.CHUNK_SIZE) * self.CHUNK_SIZE
            for i in range(self.thread_number - 1):
                self.remaining_segments.append([divided_segment_size * i, divided_segment_size * (i + 1)])
            self.remaining_segments.append([divided_segment_size * (self.thread_number - 1), self.total_size])

        self.thread_buffer_size = math.ceil(self.TOTAL_BUFFER_SIZE / self.thread_number / self.CHUNK_SIZE) * self.CHUNK_SIZE
        self.bytearray_of_threads = [None] * self.thread_number
        info_map['remaining_segments'] = self.remaining_segments
        return info_map

    def balance_remaining_segments(self, completed_index):
        copied_remaining_segments = copy.deepcopy(self.remaining_segments)
        max_remaining_index, max_remaining_task = sorted(enumerate(copied_remaining_segments), key=lambda x: x[1][1] - x[1][0], reverse=True)[0]
        remaining_bytes = max_remaining_task[1] - max_remaining_task[0]
        if remaining_bytes >= self.MIN_TASK_CHUNK_SIZE:
            new_end_of_max_remaining_task = max_remaining_task[1] - math.floor(remaining_bytes * 0.5 / self.CHUNK_SIZE) * self.CHUNK_SIZE
            new_start = new_end_of_max_remaining_task
            new_end = max_remaining_task[1]
            self.remaining_segments[max_remaining_index][1] = new_end_of_max_remaining_task
            self.remaining_segments[completed_index][0] = new_start
            self.remaining_segments[completed_index][1] = new_end
            return {'type': 'worker'}
        elif remaining_bytes > 0:
            new_start = max_remaining_task[0]
            new_end = max_remaining_task[1]
            self.remaining_segments[completed_index][0] = new_start
            self.remaining_segments[completed_index][1] = new_end
            self.coworker_end_to_indexes_map.get(new_end, (max_remaining_index, set()))[1].add(completed_index)
            return {'type': 'coworker', 'new_start': new_start, 'new_end': new_end}
        else:
            return {'type': 'stop'}

    def calculate_realtime_speed_once(self, do_init=False):
        tmp = copy.deepcopy(self.remaining_segments)
        new_tmp = []
        for [start, end] in tmp:
            if start < end:
                for index, [s, e] in enumerate(new_tmp):
                    if start < e and end > s:
                        new_tmp[index] = [max(start, s), min(end, e)]
                        break
                else:
                    new_tmp.append([start, end])
        remaining_size = sum([end - start for [start, end] in new_tmp])
        downloaded_size = self.total_size - remaining_size

        current_time = datetime.datetime.now()
        seconds = (current_time - self.start_time).seconds

        if do_init:
            self.last_progress_time = seconds + 0.001
            self.last_downloaded_size = downloaded_size
        if downloaded_size < self.last_downloaded_size:
            downloaded_size = self.last_downloaded_size
        downloaded_size_in_period = downloaded_size - self.last_downloaded_size
        self.last_downloaded_size = downloaded_size
        finish_percent = math.floor(downloaded_size / self.total_size * 10000) / 10000

        return {
            'total_size': self.total_size,
            'downloaded_size': downloaded_size,
            'downloaded_size_in_period': downloaded_size_in_period,
            'finish_percent': finish_percent,
            'realtime_speed': downloaded_size_in_period / (seconds - self.last_progress_time),
            'total_seconds': seconds
        }

    def calculate_realtime_speed(self):
        while True:
            sleep(1)
            progress = self.calculate_realtime_speed_once()
            finish_percent = progress['finish_percent']
            done = math.floor(50 * finish_percent)

            sys.stdout.write("\r[%s%s] %.2f%% %.2fMB|%.2fMB %.3fMB/s %ds" % ('█' * done, ' ' * (50 - done), 100 * finish_percent, math.floor(progress['downloaded_size'] / 1024 / 10.24) / 100, math.floor(self.total_size / 1024 / 10.24) / 100, math.floor(progress['downloaded_size_in_period'] / 1024 / 1.024) / 1000, progress['total_seconds']))
            sys.stdout.flush()

            if finish_percent == 1:
                break

    def store_segment(self, start, data):
        self.file_seeker.seek(start)
        if start + len(data) > self.total_size:
            data = data[:self.total_size-start]
        self.file_seeker.write(data)

    def store_remaining_segment(self):
        for index, segment in enumerate(self.bytearray_of_threads):
            if segment is not None:
                start, data, data_length = segment[0], segment[1], segment[2]
                if data_length > 0:
                    data = data[:data_length]
                    self.file_seeker.seek(start)
                    self.file_seeker.write(data)

    def start_download_task(self, task_index, task_type):

        (start, end) = self.remaining_segments[task_index]
        if start is None or start >= end:
            return task_index
        data = bytearray(self.thread_buffer_size)
        data_length = 0
        task_downloaded_size = 0

        self.bytearray_of_threads[task_index] = [start, data, data_length]

        while True:
            try:
                with gevent.Timeout(5):
                    headers = {**self.headers, 'Range': 'bytes=%d-%d' % (start + data_length, end - 1)}
                    r = requests.get(self.url, stream=True, verify=False, headers=headers)
                status_code = r.status_code
                assert status_code not in (200, 416)
                if status_code == 206:

                    chunk_size = min(self.CHUNK_SIZE, end - start - data_length)
                    chunks = r.iter_content(chunk_size=chunk_size)
                    while True:
                        with gevent.Timeout(5):
                            chunk = chunks.__next__()
                        get_chunk_size = len(chunk)
                        if chunk and (len(chunk) == self.CHUNK_SIZE or len(chunk) == chunk_size):
                            task_downloaded_size += get_chunk_size
                            data_length += get_chunk_size
                            end = self.remaining_segments[task_index][1]
                            self.remaining_segments[task_index][0] = start + data_length
                            data[data_length - get_chunk_size:data_length] = chunk
                            self.bytearray_of_threads[task_index][2] = data_length

                            if end - 1 <= start + data_length or data_length + self.CHUNK_SIZE > self.thread_buffer_size:
                                if end - 1 <= start + data_length:
                                    self.bytearray_of_threads[task_index] = None
                                    self.complete_notify_lock.acquire()
                                    self.to_dispatcher_queue.put({'type': task_type + '_finish_download', 'index': task_index, 'start': start, 'data': data[:data_length], 'end': end})
                                    self.complete_notify_lock.release()
                                    return
                                else:
                                    self.to_dispatcher_queue.put({'type': 'part_downloaded', 'start': start, 'data': data[:data_length]})
                                    start += data_length
                                    data = bytearray(self.thread_buffer_size)
                                    data_length = 0
                                    self.bytearray_of_threads[task_index] = [start, data, data_length]
                        else:
                            break
            except (gevent.timeout.Timeout,requests.exceptions.ProxyError, requests.exceptions.ConnectionError, StopIteration, KeyboardInterrupt):
                pass

    def store_breakpoint(self):
        self.segment_dispatch_task.kill()
        self.speed_calculation_task.kill()
        self.store_remaining_segment()
        self.calculate_realtime_speed_once()
        self.file_seeker.flush()
        self.file_seeker.close()
        with open(self.breakpoint_file_path, 'wb') as f:
            self.download_info['thread_number'] = self.thread_number
            pickle.dump(self.download_info, f)

    def remove_breakpoint_file(self):
        if os.path.exists(self.breakpoint_file_path):
            os.remove(self.breakpoint_file_path)

    def dispatch_segment(self):
        while True:
            request = self.to_dispatcher_queue.get()
            if request['type'] == 'part_downloaded':
                start, data = request['start'], request['data']
                self.store_segment(start, data)

            elif request['type'] == 'worker_finish_download':
                self.complete_notify_lock.acquire()
                completed_index, start, data, end = request['index'], request['start'], request['data'], request['end']
                self.store_segment(start, data)
                (worker_index, coworker_indexes) = self.coworker_end_to_indexes_map.get(end, (completed_index, set()))
                for index in coworker_indexes:
                    self.remaining_segments[index][1] = -1
                    self.workers[index].kill()
                res = self.balance_remaining_segments(worker_index)
                if res['type'] == 'stop':
                    return
                elif res['type'] == 'worker':
                    self.workers[worker_index] = gevent.spawn(self.start_download_task, worker_index, res['type'])
                elif res['type'] == 'coworker':
                    for index in {worker_index} | coworker_indexes:
                        self.workers[index] = gevent.spawn(self.start_download_task, index, res['type'])
                self.complete_notify_lock.release()

            elif request['type'] == 'coworker_finish_download':
                self.complete_notify_lock.acquire()
                completed_index, completed_start, completed_data, completed_end = request['index'], request['start'], request['data'], request['end']
                self.store_segment(completed_start, completed_data)
                (worker_index, coworker_indexes) = self.coworker_end_to_indexes_map.get(completed_end)
                [worker_start, worker_data, _] = self.bytearray_of_threads[worker_index]
                self.store_segment(worker_start, worker_data[:completed_start - worker_start])
                for index in coworker_indexes - {completed_index}:
                    self.remaining_segments[index][1] = -1
                    self.workers[index].kill()
                res = self.balance_remaining_segments(worker_index)
                if res['type'] == 'stop':
                    return
                elif res['type'] == 'coworker':
                    for index in {worker_index} | coworker_indexes:
                        self.remaining_segments[index][0] = res['new_start']
                        self.remaining_segments[index][1] = res['new_end']
                        self.workers[index] = gevent.spawn(self.start_download_task, index, res['type'])
                self.complete_notify_lock.release()

    def download(self):

        self.start_time = datetime.datetime.now()
        self.calculate_realtime_speed_once(do_init=True)
        if self.print_progress:
            self.speed_calculation_task = gevent.spawn(self.calculate_realtime_speed)
        self.segment_dispatch_task = gevent.spawn(self.dispatch_segment)

        for index in range(self.thread_number):
            self.workers.append(gevent.spawn(self.start_download_task, index, 'worker'))
        try:
            if self.print_progress:
                self.speed_calculation_task.join()
            self.segment_dispatch_task.kill()
            self.file_seeker.flush()
            self.file_seeker.close()
            self.remove_breakpoint_file()
        except KeyboardInterrupt:
            self.store_breakpoint()
        sys.stdout.write('\n')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", '--thread_number', type=int, dest="thread_number", help="Thread number to download", default=32)
    parser.add_argument("-p", "--path", type=str, dest="path",  help="Path to store file", default=None)
    parser.add_argument('-f', "--file_name", type=str, dest="file_name", help='Filename to save', default=None)
    parser.add_argument('url')
    args = parser.parse_args()

    HttpMultiThreadDownloader(url=args.url, thread_number=args.thread_number, file_name=args.file_name, path_to_store=args.path, print_progress=True).download()