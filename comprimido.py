#!/usr/bin/env python
from mpi4py import MPI
from geoip2.errors import AddressNotFoundError
import geoip2.database, gzip
from time import time
#import pygeoip

#Inicializar el COMM_WORLD y sus mecanismos
comm = MPI.COMM_WORLD
rank = comm.rank
size = comm.Get_size()
name = MPI.Get_processor_name()


#Variables Globales
tiempo_inicial = time()
listaArchivos = ["audit.log.2018-10-04.gz", "audit.log.2018-10-03.gz", "audit.log.2018-10-02.gz"]
arregloPrincipal = []
arregloIp = []
contadorIp = []
arregloIpLocal = []
contadorIpLocal = []
arregloPais = []
contadorPais = []
arregloCiudad = []
contadorCiudad = []
arregloHora = []
contadorHora = []
arregloCorreo = []
contadorCorreo = []
arregloProtocolo = []
contadorProtocolo = []
arregloFecha = []
contadorFecha = []

fragmento = 0

#Verifica cuantos procesos son y crea el número de arreglos necesarios
def crear_lista_procesos(size):
    global fragmento
    x = 0
    while(x<size):
        arregloPrincipal.append([])
        x += 1
    fragmento = size-1

#Se lee el archivo de logs, seguidamente se verifican cuales son WARN y se añaden a la lista
def leer_logs():
    arregloLogs = []#arreglo que almacena todos los logs
    for x in listaArchivos:#Leer todos los archivos de LOGS
        archivo = gzip.GzipFile(fileobj=open(x, 'rb'))
        cadena = archivo.read()
        arregloLogs = arregloLogs + cadena.split('\n')
        archivo.close()
    arregloWarns = []#arreglo que almacena solo los logs del tipo WARN
    for x in arregloLogs:
        if 'WARN' in x:
            arregloWarns.append(x)
    global fragmento
    x,y = 0,0
    while(x<len(arregloWarns)):
        if(y>fragmento):
            y=0
        arregloPrincipal[y].append(arregloWarns[x])
        y += 1
        x += 1

#Organizamos las lineas
def organizar(arreglo, principal, contador):
    longitud = len(arreglo)
    x = 0
    while(x<longitud):
        arreglo1 = arreglo[x]
        arreglo2 = len(arreglo1)
        y = 0
        while(y<arreglo2):
            w = arreglo1[y]
            if w in principal:
                index = principal.index(w)
                contador[index] += 1
            else:
                principal.append(w)
                contador.append(1)
            y += 1
        x += 1 #Una menos didentacion

#Organizamos de forma descendente
def top20(principal, contador):
    for i in range(len(contador)-1,0,-1):
        for j in range(i):
            if contador[j]>contador[j+1]:
                temporal=contador[j]
                temporal1=principal[j]
                contador[j]=contador[j+1]
                principal[j]=principal[j+1]
                contador[j+1]=temporal
                principal[j+1]=temporal1
    i = 0
    j = len(contador)-1
    temporal = []
    temporal1 = []
    while(i<len(contador)):
        temporal.append(contador[j])
        temporal1.append(principal[j])
        i += 1
        j -= 1
    i = 0
    while(i<len(contador)):
        principal[i] = temporal1[i]
        contador[i] = temporal[i]
        i += 1

#Funcion que muestra los resultados de cada TOP 20
def resultado(principal, contador):
    x = 0
    y = 0
    sum = 0
    longitud = len(contador)
    if(longitud>20):
        longitud = 20
    while(y<len(contador)):
        sum = sum+contador[y]
        y += 1
    while(x<longitud):
        p = round((contador[x]/float(sum))*100,2)
        print(str(x+1) + ". ---- " + principal[x] + " ---- Ataques: " + str(contador[x]) + " ---- Porcentaje: " + str(p) + "%")
        x += 1

################################### M P I ########################################
if(rank == 0):#El maestro se encarga de leer los archivos y mandarlos a los otros procesos
    crear_lista_procesos(size)
    leer_logs()
    print('\nNodo Maestro dice --> Termine de leer Logs')
    archivo.close()
    print('\n Nodo Maestro dice --> Empezando el Scatter')
else:
    arregloPrincipal = None

#------Codigo Paralelizado--------------
scatter = comm.scatter(arregloPrincipal, root=0)

#Procesamiento

longitud = len(scatter)
x = 0
listaIp = []
listaIpLocal = []
listaPais = []
listaCiudad = []
listaProtocolo = []
listaHora = []
listaCorreo = []
listaFecha = []
for x in range(longitud):
    info = scatter[x]
    confirma = info[24:28]
    fecha = info[0:10]
    ip1 = info.split("oip=")
    ip2 = ip1[1].split(";")
    ip = ip2[0]
    hora = info[11:13]
    correo1 = info.split("account=")
    correo2 = correo1[1].split(";")
    correo = correo2[0]
    protocolo1 = info.split("protocol=")
    protocolo2 = protocolo1[1].split(";")
    protocolo = protocolo2[0]
    confirmaIpLocal = ip[0:7]
    cofirmaIpNone = ip[0:6]
    if(confirmaIpLocal == "192.168"):
        listaIpLocal.append(ip)
        listaCiudad.append("LocalHost")
        listaPais.append("LocalHost")
    else:
        reader = geoip2.database.Reader('/home/group/distribuidos/201915_15579/10/workspace/GeoLite2-City_20181113/GeoLite2-City.mmdb')
        response = reader.city(ip)
        listaPais.append(format(response.country.name))
        listaCiudad.append(format(response.city.name))
        listaIp.append(ip)
        listaHora.append(hora)
        listaFecha.append(fecha)
        listaCorreo.append(correo)
        listaProtocolo.append(protocolo)
print('Termine, soy el proceso: ' +str(rank)+ ' del nodo: '+name)

#Devolver Scatter con Gatter
ips = comm.gather(listaIp, root=0)
ipsLocal = comm.gather(listaIpLocal, root=0)
paises = comm.gather(listaPais, root=0)
ciudades = comm.gather(listaCiudad, root=0)
horas = comm.gather(listaHora, root=0)
correos = comm.gather(listaCorreo, root=0)
protocolos = comm.gather(listaProtocolo, root=0)
fechas = comm.gather(listaFecha, root=0)

if(rank ==0):
    organizar(ips, arregloIp, contadorIp)
    organizar(ipsLocal, arregloIpLocal, contadorIpLocal)
    organizar(paises, arregloPais, contadorPais)
    organizar(ciudades, arregloCiudad, contadorCiudad)
    organizar(horas, arregloHora, contadorHora)
    organizar(correos, arregloCorreo, contadorCorreo)
    organizar(protocolos, arregloProtocolo, contadorProtocolo)
    organizar(fechas, arregloFecha, contadorFecha)
    top20(arregloIp, contadorIp)
    top20(arregloIpLocal, contadorIpLocal)
    top20(arregloPais, contadorPais)
    top20(arregloCiudad, contadorCiudad)
    top20(arregloHora, contadorHora)
    top20(arregloCorreo, contadorCorreo)
    top20(arregloProtocolo, contadorProtocolo)
    top20(arregloFecha, contadorFecha)

    print('')

    print('  #####  #       #     #  #####  ####### ####### ######     ######  ####### ####### #     # ### #     # #######  ')
    print(' #    #  #       #     # #     #    #    #       #     #    #     # #       #       #     #  #  #     # #        ')
    print(' #       #       #     # #          #    #       #     #    #     # #       #       #     #  #  #     # #        ')
    print(' #       #       #     #  #####     #    #####   ######     ######  #####   #####   #######  #  #     # #####    ')
    print(' #       #       #     #       #    #    #       #   #      #     # #       #       #     #  #   #   #  #        ')
    print(' #    #  #       #     # #     #    #    #       #    #     #     # #       #       #     #  #    # #   #        ')
    print(' #####  #######  #####   #####     #    ####### #     #    ######  ####### ####### #     # ###    #    #######   ')

    print('                                    #     #  #####     #    ######  ')
    print('                                    #     # #     #   # #   #     # ')
    print('                                    #     # #        #   #  #     # ')
    print('                                    #     # #       #     # ######  ')
    print('                                    #     # #       ####### #     # ')
    print('                                    #     # #     # #     # #     # ')
    print('                                     #####   #####  #     # ######  ')


    print('\n-------------------------------------------------------------------------------------------------')
    print('--------------------------------- Direcciones IP Que Màs Atacan ---------------------------------')
    resultado(arregloIp,contadorIp)
    print('\n-------------------------------------------------------------------------------------------------')
    print('------------------------------------- Paises Que Màs Atacan -------------------------------------')
    resultado(arregloPais,contadorPais)
    print('\n-------------------------------------------------------------------------------------------------')
    print('------------------------------------ Ciudades Que Màs Atacan ------------------------------------')
    resultado(arregloCiudad,contadorCiudad)
    print('\n-------------------------------------------------------------------------------------------------')
    print('------------------------------------ Horas En Que Màs Atacan ------------------------------------')
    resultado(arregloHora,contadorHora)
    print('\n-------------------------------------------------------------------------------------------------')
    print('------------------------------ Correos Electrònicos Que Màs Atacan ------------------------------')
    resultado(arregloCorreo,contadorCorreo)
    print('\n-------------------------------------------------------------------------------------------------')
    print('-------------------------------------- Protocolos De Ataque -------------------------------------')
    resultado(arregloProtocolo,contadorProtocolo)
    print('\n-------------------------------------------------------------------------------------------------')
    print('-------------------------------------- Fechas De Ataque -------------------------------------')
    resultado(arregloFecha,contadorFecha)
    print('\n')
    tiempo_final = time()
    tiempo_ejecucion = tiempo_final - tiempo_inicial
    print ('El tiempo de ejecucion fue: '+str(tiempo_ejecucion)+' Seg') #En segundos

