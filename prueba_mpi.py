#!/usr/bin/env python
"""
Parallel Hello World
"""

from mpi4py import MPI
import sys
import gzip

logs = []
nombreArchivos = ['/home/public/201915/muestra1/audit.log.2018-10-03.gz']
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
name = MPI.Get_processor_name()

def llenar_logs():
    if rank == 0:
        f = ''
        global logs
        for x in nombreArchivos:
            f=gzip.GzipFile(fileobj=open(x, 'rb'))
            cadena = f.read()
            logs += cadena.split('\n')
            f.close()
        comm.bcast('completeLogs', 0)
    else:
        data = comm.recv(source=0)
        print('Soy ',rank,' y recibi ',data)
 
if __name__ == '__main__':
    llenar_logs()
