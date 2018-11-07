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
    data = logs
    comm.send(data[0:1], dest=1, tag=11)
    comm.send(data[1:2], dest=2, tag=12)
    comm.send(data[2:3], dest=3, tag=13)
    comm.send(data[3:4], dest=4, tag=14)
    comm.send(data[4:5], dest=5, tag=15)
    comm.send(data[5:6], dest=6, tag=16)
    comm.send(data[6:7], dest=7, tag=17)
    comm.send(data[7:8], dest=8, tag=18)
    print 'Del Rank',name,'Enviamos las lineas de logs a procesar'
if rank == 1:
    data = comm.recv(source=0, tag=11)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:',data
if rank == 2:
    data = comm.recv(source=0, tag=12)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:',data
if rank == 3:
    data = comm.recv(source=0, tag=13)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:',data
if rank == 4:
    data = comm.recv(source=0, tag=14)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:',data
if rank == 5:
    data = comm.recv(source=0, tag=15)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:',data
if rank == 6:
    data = comm.recv(source=0, tag=16)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:',data
if rank == 7:
    data = comm.recv(source=0, tag=17)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:',data
if rank == 8:
    data = comm.recv(source=0, tag=18)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:',data
