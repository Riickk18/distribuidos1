#!/usr/bin/env python

from mpi4py import MPI
import sys
import gzip

logs = []
nombreArchivos = ['/home/public/201915/muestra1/audit.log.2018-10-03.gz','/home/public/201915/muestra1/audit.log.2018-10-02.gz','/home/public/201915/muestra1/audit.log.2018-10-04.gz']
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

def picar_ip(data):
        arregloips = []
        arregloemails = []
        arregloTime = []
        for x in data:
            #extraer ip
            inicioip = x.find('oip')+4
            finip = x[inicioip:inicioip+20].find(';')+inicioip
            if x[inicioip:finip] != '':
                arregloips.append(x[inicioip:finip])
            #extraer correo
            inicioem = x.find('account')+8
            finem = x[inicioem:].find(';')+inicioem
            if x[inicioem:finem] != '':
                arregloemails.append(x[inicioem:finem])
            #extraer horas
            finTime = x.find(',')
            inicioTime = finTime-8
            if x[inicioTime:finTime] != '':
                arregloTime.append(x[inicioTime:finTime])
        return arregloips, arregloemails, arregloTime

if rank == 0:
    llenar_logs()
    comm.send(logs[0:10], dest=1, tag=11)
    comm.send(logs[11:20], dest=2, tag=12)
    comm.send(logs[21:30], dest=3, tag=13)
    comm.send(logs[31:40], dest=4, tag=14)
    comm.send(logs[41:50], dest=5, tag=15)
    comm.send(logs[51:60], dest=6, tag=16)
    comm.send(logs[61:70], dest=7, tag=17)
    print 'Del Rank',name,'Enviamos las lineas de logs a procesar'
if rank == 1:
    data = comm.recv(source=0, tag=11)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:',data
    retorno = picar_ip(data)
    print retorno
if rank == 2:
    data = comm.recv(source=0, tag=12)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:',data
    retorno = picar_ip(data)
    print retorno
if rank == 3:
    data = comm.recv(source=0, tag=13)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:',data
    retorno = picar_ip(data)
    print retorno
if rank == 4:
    data = comm.recv(source=0, tag=14)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:',data
    retorno = picar_ip(data)
    print retorno
if rank == 5:
    data = comm.recv(source=0, tag=15)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:',data
    retorno = picar_ip(data)
    print retorno
if rank == 6:
    data = comm.recv(source=0, tag=16)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:',data
    retorno = picar_ip(data)
    print retorno
if rank == 7:
    data = comm.recv(source=0, tag=17)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:',data
    retorno = picar_ip(data)
    print retorno
