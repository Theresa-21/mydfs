import os
import socket
import time
from io import StringIO
import threading
import numpy as np
from queue import Queue
import pickle
import base64

import pandas as pd

from common import *


class Client:
    def __init__(self):
        self.name_node_sock = socket.socket()
        self.name_node_sock.connect((NAME_NODE_HOST, NAME_NODE_PORT))
    
    def __del__(self):
        self.name_node_sock.close()
    
    def ls(self, dfs_path):
        # TODO: 向NameNode发送请求，查看dfs_path下文件或者文件夹信息
        try:
            content = f"ls {dfs_path}"
            self.name_node_sock.send(bytes(content, encoding='utf-8'))
            print(str(self.name_node_sock.recv(BUF_SIZE), encoding='utf-8'))
        except Exception as e:
            print(e)

    def copyFromLocal(self, local_path, dfs_path):
        file_size = os.path.getsize(local_path)
        print("File size: {}".format(file_size))
        
        request = "new_fat_item {} {}".format(dfs_path, file_size)
        print("Request: {}".format(request))
        
        # 从NameNode获取一张FAT表
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)
        
        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        
        # 根据FAT表逐个向目标DataNode发送数据块
        fp = open(local_path)
        for idx, row in fat.iterrows():
            data = fp.read(int(row['blk_size']))
            
            data_node_sock = socket.socket()
            data_node_sock.connect((row['host_name'], DATA_NODE_PORT))
            blk_path = dfs_path + ".blk{}".format(row['blk_no'])
            
            request = "store {}".format(blk_path)
            data_node_sock.send(bytes(request, encoding='utf-8'))
            time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包
            data_node_sock.send(bytes(data, encoding='utf-8'))
            data_node_sock.close()
        fp.close()
    
    def copyToLocal(self, dfs_path, local_path):
        request = "get_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))
        # TODO: 从NameNode获取一张FAT表；打印FAT表；根据FAT表逐个从目标DataNode请求数据块，写入到本地文件中
        try:
            # 1. 接收并解析FAT表
            fat_data = str(self.name_node_sock.recv(BUF_SIZE), encoding='utf-8')
            print(f"FAT Table:\n{fat_data}")
            fat_table = pd.read_csv(StringIO(fat_data))

            # 2. 按数据块下载并写入文件
            with open(local_path, 'w') as file:
                processed_blocks = set()
                
                for _, block_info in fat_table.iterrows():
                    block_id = block_info['blk_no']

                    # 跳过已读文件
                    if block_id in processed_blocks:
                        continue

                    processed_blocks.add(block_id)
                    with socket.socket() as dn_socket:
                        dn_socket.connect((block_info['host_name'], DATA_NODE_PORT))
                        request = f"load {dfs_path}.blk{block_id}"
                        dn_socket.send(bytes(request, 'utf-8'))
                        time.sleep(0.1)
                        chunk_data = str(dn_socket.recv(BUF_SIZE), encoding='utf-8')
                        file.write(chunk_data)
        except Exception as e:
            print(e)
        

    def rm(self, dfs_path):
        request = "rm_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))
        # TODO: 从NameNode获取改文件的FAT表，获取后删除；打印FAT表；根据FAT表逐个告诉目标DataNode删除对应数据块
        try:
            # 1. 发送请求并接收FAT表
            self.name_node_sock.send(bytes(request, 'utf-8'))
            fat_data = str(self.name_node_sock.recv(BUF_SIZE), 'utf-8')

            print(f"FAT Table:\n{fat_data}")
            fat_table = pd.read_csv(StringIO(fat_data))

            # 2. 遍历FAT表删除所有数据块
            for _, block_info in fat_table.iterrows():
                with socket.socket() as dn_socket:
                    dn_socket.connect((block_info['host_name'], DATA_NODE_PORT))
                    
                    block_path = f"{dfs_path}.blk{block_info['blk_no']}"
                    dn_socket.send(bytes(f"rm {block_path}", 'utf-8'))
                    
                    response = str(dn_socket.recv(BUF_SIZE), 'utf-8')
                    print(response)
        except Exception as e:
            print(e)
        
    def format(self):
        request = "format"
        print(request)
        
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        print(str(self.name_node_sock.recv(BUF_SIZE), encoding='utf-8'))
        
        for host in HOST_LIST:
            data_node_sock = socket.socket()
            data_node_sock.connect((host, DATA_NODE_PORT))
            
            data_node_sock.send(bytes("format", encoding='utf-8'))
            print(str(data_node_sock.recv(BUF_SIZE), encoding='utf-8'))
            
            data_node_sock.close()
            
    def task_runner(self, server_addr, segment_start, segment_end, file_path, result_collector):
        print(f"Invoking server {server_addr} for mapping task...")
        client_socket = socket.socket()
        client_socket.connect((server_addr, DATA_NODE_PORT))
        command = "map {} {} {}".format(file_path, segment_start, segment_end)
        client_socket.send(bytes(command, encoding='utf-8'))

        # 确保完整接收数据包
        received_data = b''
        while True:
            chunk = client_socket.recv(BUF_SIZE)
            if not chunk:
                break
            received_data += chunk
        client_socket.close()

        # 反序列化处理后的映射结果
        server_result = pickle.loads(base64.b64decode(received_data))
        
        # 执行数据重分配
        with threading.Lock():
            for key, value_list in server_result.items():
                result_collector[key] = result_collector.get(key, []) + value_list

    def distributed_matrix_computation(self, input_file, output_file):
        # 从主节点获取矩阵维度信息
        master_socket = socket.socket()
        master_socket.connect((HOST_LIST[0], DATA_NODE_PORT))
        master_socket.send(bytes(f"info {input_file}", encoding='utf-8'))
        response = str(master_socket.recv(BUF_SIZE), encoding='utf-8')
        rows_m, cols_n, cols_p, total_lines = tuple(map(int, response.split(' ')))

        host_count = len(HOST_LIST)
        aggregated_results = {}
        
        begin_time = time.time()
        # 小规模数据直接单节点处理
        if host_count > total_lines:   
            single_socket = socket.socket()
            single_socket.connect((HOST_LIST[0], DATA_NODE_PORT))
            command = "map {} {} {}".format(input_file, 0, total_lines)
            single_socket.send(bytes(command, encoding='utf-8'))
            raw_data = b''
            while True:
                data_chunk = single_socket.recv(BUF_SIZE)
                if not data_chunk:
                    break
                raw_data += data_chunk
            single_socket.close()
            aggregated_results = pickle.loads(base64.b64decode(raw_data))
        else:
            # 多节点并行处理
            segment_size = total_lines // host_count
            worker_threads = []
            for i, server in enumerate(HOST_LIST):
                start_index = i * segment_size
                end_index = (i + 1) * segment_size if (i + 1) * segment_size < total_lines else total_lines
                worker = threading.Thread(target=self.task_runner, args=(server, start_index, end_index, input_file, aggregated_results))
                worker_threads.append(worker)
                worker.start()

            for worker in worker_threads:
                worker.join()

        print(f"Execution Time: {time.time() - begin_time:.2f}s")
        
        # 并行处理子数据集
        def compute_segment(data_segment, output_queue):
            segment_output = []
            
            # 使用DataFrame进行高效计算
            for matrix_key in sorted(data_segment):
                value_array = data_segment[matrix_key]
                data_frame = pd.DataFrame(value_array, columns=['mat_id', 'index_key', 'val'])
                if len(data_frame['mat_id'].unique()) == 1:
                    continue
                
                grouped_data = data_frame.groupby(['mat_id', 'index_key'])
                product_result = grouped_data['val'].prod().unstack(fill_value=0)
                final_sum = (product_result.iloc[0] * product_result.iloc[1]).sum()
                
                if final_sum != 0:
                    segment_output.append([matrix_key[0], matrix_key[1], final_sum])

            output_queue.put(segment_output)

        # 多线程结果归约
        def result_aggregator(input_data):
            sorted_keys = sorted(input_data.keys())
            thread_count = min(len(sorted_keys), os.cpu_count() - 1)
            keys_per_thread = len(sorted_keys) // thread_count
            results_queue = Queue()

            compute_threads = []
            for j in range(thread_count):
                start_pos = j * keys_per_thread
                end_pos = start_pos + keys_per_thread if j != thread_count - 1 else len(sorted_keys)
                key_subset = {key: input_data[key] for key in sorted_keys[start_pos:end_pos]}
                compute_thread = threading.Thread(target=compute_segment, args=(key_subset, results_queue))
                compute_threads.append(compute_thread)
                compute_thread.start()

            for compute_thread in compute_threads:
                compute_thread.join()

            combined_results = []
            while not results_queue.empty():
                combined_results.extend(results_queue.get())
                
            combined_results = sorted(combined_results, key=lambda elem: (elem[0], elem[1]))

            return np.array(combined_results)

        np.savetxt(output_file, result_aggregator(aggregated_results), fmt="%d", delimiter=',')

# 解析命令行参数并执行对于的命令
import sys

argv = sys.argv
argc = len(argv) - 1

client = Client()

cmd = argv[1]
if cmd == '-ls':
    if argc == 2:
        dfs_path = argv[2]
        client.ls(dfs_path)
    else:
        print("Usage: python client.py -ls <dfs_path>")
elif cmd == "-rm":
    if argc == 2:
        dfs_path = argv[2]
        client.rm(dfs_path)
    else:
        print("Usage: python client.py -rm <dfs_path>")
elif cmd == "-copyFromLocal":
    if argc == 3:
        local_path = argv[2]
        dfs_path = argv[3]
        client.copyFromLocal(local_path, dfs_path)
    else:
        print("Usage: python client.py -copyFromLocal <local_path> <dfs_path>")
elif cmd == "-copyToLocal":
    if argc == 3:
        dfs_path = argv[2]
        local_path = argv[3]
        client.copyToLocal(dfs_path, local_path)
    else:
        print("Usage: python client.py -copyFromLocal <dfs_path> <local_path>")
elif cmd == "-format":
    client.format()
elif cmd == "-matrix":
    if argc == 3:
        input_path = argv[2]
        output_path = argv[3]
        client.distributed_matrix_computation(input_path, output_path)
    else:
        print("Usage: python client.py -matrix <input_path> <output_path>")
else:
    print("Undefined command: {}".format(cmd))
    print("Usage: python client.py <-ls | -copyFromLocal | -copyToLocal | -rm | -format> other_arguments")
