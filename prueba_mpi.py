#!/usr/bin/env python
"""
Parallel Hello World
"""

from mpi4py import MPI
import sys
import gzip

logs = []
nombreArchivos = ['/home/public/201915/muestra1/audit.log.2018-10-03.gz']

def llenar_logs():
       f = ''
       global logs
       for x in nombreArchivos:
           f=gzip.GzipFile(fileobj=open(x, 'rb'))
           cadena = f.read()
           logs += cadena.split('\n')
           f.close()
       print(logs[0:3])

size = MPI.COMM_WORLD.Get_size()
rank = MPI.COMM_WORLD.Get_rank()
name = MPI.Get_processor_name()

if rank == 0:
   sys.stdout.write('Soy nodo maestro %d \n' % (rank))
if rank == 1:
   print('Soy el Proceso %d \n' % (rank))
   llenar_logs()
if rank == 2:
   print('Soy el Proceso %d \n' % (rank))
   llenar_logs()
if rank == 3:
   print('Soy el Proceso %d \n' % (rank))
   llenar_logs()
if rank == 4:
   print('Soy el Proceso %d \n' % (rank))
   llenar_logs()
if rank == 5:
   print('Soy el Proceso %d \n' % (rank))
   llenar_logs()
if rank == 6:
   print('Soy el Proceso %d \n' % (rank))
   llenar_logs()
if rank == 7:
   print('Soy el Proceso %d \n' % (rank))
   llenar_logs()
if rank == 8:
   print('Soy el Proceso %d \n' % (rank))
   llenar_logs()
