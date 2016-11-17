import os, sys
from multiprocessing import Pool
import paramiko

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

rooms = sys.argv[1:]
max = 40

def ping(room_i):
    try:
        room = room_i[0]
        i = room_i[1]
        host = 'c' + str(room) + '-' + str(i).zfill(2)
        ssh.connect(host, timeout=1)
        ssh.close()
        return host
    except:
        pass

pool = Pool(10)

hosts = pool.map(ping, [[room, i] for room in rooms for i in range(max)])
hosts_not_null = [h for h in hosts if h is not None]

f = open('resources/hosts', 'w+')
f.write('\n'.join(hosts_not_null))
f.close()