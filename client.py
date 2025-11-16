import os
import socket
import time
from io import StringIO

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
else:
    print("Undefined command: {}".format(cmd))
    print("Usage: python client.py <-ls | -copyFromLocal | -copyToLocal | -rm | -format> other_arguments")
