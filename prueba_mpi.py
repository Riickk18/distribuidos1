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
        f = ''
        global logs
        for x in nombreArchivos:
            f=gzip.GzipFile(fileobj=open(x, 'rb'))
            cadena = f.read()
            logs += cadena.split('\n')
            f.close()

if rank == 0:
    llenar_logs()
    data = 'Archivos de Logs Procesados'
    comm.send(data, dest=1)
    comm.send(data, dest=2)
    comm.send(data, dest=3)
    comm.send(data, dest=4)
    comm.send(data, dest=5)
    comm.send(data, dest=6)
    comm.send(data, dest=7)
    comm.send(data, dest=8)
print 'Del Rank',name,'Enviamos',data
if rank == 1:
    data = comm.recv(source=0)
    print 'En Nodo',name, 'Recibimos:',data
if rank == 2:
    data = comm.recv(source=0)
    print 'En Nodo',name, 'Recibimos:',data
if rank == 3:
    data = comm.recv(source=0)
    print 'En Nodo',name, 'Recibimos:',data
if rank == 4:
    data = comm.recv(source=0)
    print 'En Nodo',name, 'Recibimos:',data
if rank == 5:
    data = comm.recv(source=0)
    print 'En Nodo',name, 'Recibimos:',data
if rank == 6:
    data = comm.recv(source=0)
    print 'En Nodo',name, 'Recibimos:',data
if rank == 7:
    data = comm.recv(source=0)
    print 'En Nodo',name, 'Recibimos:',data
if rank == 8:
    data = comm.recv(source=0)
    print 'En Nodo',name, 'Recibimos:',data
