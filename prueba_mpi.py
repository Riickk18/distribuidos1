#!/usr/bin/env python

from mpi4py import MPI
import sys, gzip, re

logs = []
nombreArchivos = ['/home/public/201915/muestra1/audit.log.2018-10-03.gz','/home/public/201915/muestra1/audit.log.2018-10-02.gz','/home/public/201915/muestra1/audit.log.2018-10-04.gz']
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
name = MPI.Get_processor_name()

def llenar_logs():
    f = ''
    logs = []
    contador = 1
    for x in nombreArchivos:
        f=gzip.GzipFile(fileobj=open(x, 'rb'))
        cadena = f.read()
        logs += cadena.split('\n')
        print('Archivo ' + str(contador)+', cantidad de lineas: '+str(len(logs)))
        f.close()
    print(len(logs))
    return logs

def llenar_logs_Warn():
    f = ''
    arregloWarns = []
    global logs
    for x in nombreArchivos:
        f=gzip.GzipFile(fileobj=open(x, 'rb'))
        cadena = f.read()
        logs += cadena.split('\n')
        f.close()
    for x in range(len(logs)-1):
        if 'WARN' in logs[x]:
            arregloWarns.append(logs[x])
    return arregloWarns


def sacar_top20(arreglo):
    diccionario = {}
    arregloTop20 = []
    arregloClave=[]
    arregloValor=[]
    for elemento in arreglo:
        if elemento in diccionario:#le sumo uno a un elemento existente
            diccionario[elemento] += 1
        else:#Creo el elemento en el diccionario
            diccionario[elemento] = 1
    tuplas = diccionario.items()
    for x in tuplas:
        arregloClave.append(x[0])
        arregloValor.append(x[1])
    for x in range(20):
        numeroMayor = arregloValor[0]
        posicionMayor = 0
        for y in range(1,len(arregloValor)-1):
            if arregloValor[y] > numeroMayor:
                numeroMayor = arregloValor[y]
                posicionMayor = y
        arregloTop20.append([arregloClave[posicionMayor], arregloValor[posicionMayor]])
        arregloClave.pop(y)
        arregloValor.pop(y)

    # claveMayor = max(diccionario.keys())
    # arregloTop20.append(diccionario[claveMayor])
    # del diccionario[claveMayor]
    return arregloTop20

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
        correo = x[inicioem:finem]
        if correo != '' and re.match('^[(a-z0-9\_\-\.)]+@[(a-z0-9\_\-\.)]+\.[(a-z)]{2,15}$',correo.lower()):
            arregloemails.append(x[inicioem:finem])
        #extraer horas
        finTime = x.find(',')
        inicioTime = finTime-8
        if x[inicioTime:finTime] != '':
            arregloTime.append(x[inicioTime:finTime])
    return sacar_top20(arregloips),sacar_top20(arregloemails), sacar_top20(arregloTime)

def partir_logs():
    tamano = len(logs)/(size-1)
    tamano = int(tamano)
    arregloRetorno = []
    for x in range(size-2):
        if x == 0:
            arregloRetorno.append([0,tamano])
        else:
            if x != size-2:
                arregloRetorno.append([(tamano*x)+1,tamano*(x+1)])
            else:
                arregloRetorno.append([(tamano*x)+1, len(logs)])
    return arregloRetorno

if rank == 0:
    print 'Del Rank',name,'Enviamos las lineas de logs a procesar'
    logs = llenar_logs()
    #indice = partir_logs()
    #contador = 1
    #while (contador < 8):
        #comm.send(logs[indice[contador-1][0]:indice[contador-1][1]], dest=contador, tag=contador+10)
        #contador += 1
    comm.send(logs[0:100], dest=1, tag=11)
    comm.send(logs[101:200], dest=2, tag=12)
    comm.send(logs[201:300], dest=3, tag=13)
    comm.send(logs[301:400], dest=4, tag=14)
    comm.send(logs[401:500], dest=5, tag=15)
    comm.send(logs[501:600], dest=6, tag=16)
    comm.send(logs[601:700], dest=7, tag=17)
if rank == 1:
    data = comm.recv(source=0, tag=11)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:'
    retorno = picar_ip(data)
    print retorno
if rank == 2:
    data = comm.recv(source=0, tag=12)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:'
    retorno = picar_ip(data)
    #print retorno
if rank == 3:
    data = comm.recv(source=0, tag=13)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:'
    retorno = picar_ip(data)
    #print retorno
if rank == 4:
    data = comm.recv(source=0, tag=14)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:'
    retorno = picar_ip(data)
    #print retorno
if rank == 5:
    data = comm.recv(source=0, tag=15)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:'
    retorno = picar_ip(data)
    #print retorno
if rank == 6:
    data = comm.recv(source=0, tag=16)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:'
    retorno = picar_ip(data)
    #print retorno
if rank == 7:
    data = comm.recv(source=0, tag=17)
    print 'En Nodo de Nombre ',name, 'Con Rango ', rank, 'Recibimos:'
    retorno = picar_ip(data)
    #print retorno
