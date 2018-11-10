import socket
import time

server = socket.socket()
#绑定要监听端口
server.bind(('localhost', 9999))
#开始监听
server.listen()
#获取客户端标识和地址
conn,addr = server.accept()
#从文件读取数据
with  open('data3.json', 'r') as f:
    while True:
        data = f.readline()
        if data.strip()=="":
            break
        conn.send(data.encode())
        time.sleep(1)# 注释或改为0出错 ？
#关闭连接
#server.close()