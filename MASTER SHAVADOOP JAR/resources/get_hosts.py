import os, sys
from multiprocessing import Pool
import paramiko

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

room=sys.argv[1]
max = 40

def ping(i):
    try:
        host = 'c' + str(room) + '-' + str(i).zfill(2)
        ssh.connect(host, timeout=3)
        ssh.close()
        return host
    except:
        pass

pool = Pool(10)
hosts = [x for x in pool.map(ping, [i for i in range(max)]) if x is not None]

f = open('/cal/homes/aroville/workspace/MASTER SHAVADOOP JAR/resources/ips_' + room, 'w+')
f.write('\n'.join(hosts))
f.close()