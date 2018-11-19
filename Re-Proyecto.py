#!/usr/bin/env python
from mpi4py import MPI
from geoip2.errors import AddressNotFoundError
import geoip2.database
#import pygeoip

#Inicializar el COMM_WORLD y sus mecanismos
comm = MPI.COMM_WORLD
rank = comm.rank
size = comm.Get_size()

#Arreglo Principal
arregloPrincipal = [] #Vectormaster

#Arreglos y Contadores de algunos Arreglos
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

#Fragmento
fragmento = 0

#Verifica cuantos procesos son y crear Arreglos necesarios
def picar(size):
    global fragmento
    x = 0
    while(x<size):
        arregloPrincipal.append([])
        x += 1
    fragmento = size-1

#Leemos los Logs e ingresa en el arreglo
def leerLog(archivo, longitud):
    global fragmento
    x = 0
    y = 0
    while(x<longitud):
        if(y>fragmento):
            y = 0
        txt = archivo.readline()
        arregloPrincipal[y].append(txt)
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

#Mostramos
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
        print(str(x+1) + ". - " + principal[x] + ". - Ataques: " + str(contador[x]) + ". - Porcentaje: " + str(p))
        x += 1

################################### M P I ########################################
if(rank == 0):
    picar(size)
    archivo = open("audit.log.2018-10-04", "r")
    longitud = len(open("audit.log.2018-10-04").readline())
    leerLog(archivo, longitud)
    archivo.close()
else:
    arregloPrincipal = None

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
while(x<longitud):
    info = scatter[x]
    confirma = info[24:28]
    if(confirma == "WARN"):
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
            #gi = pygeoip.GeoIP('GeoIPCity.dat')
            #ciudad = gi.time_zone_by_addr(ip)
            #listaCiudad.append(ciudad)
            #listaCiudad.append(gi.time_zone_by_addr(ip))
            reader = geoip2.database.Reader('/home/public/examples/geoip_test/GeoLite2-Country_20181023/GeoLite2-Country.mmdb')
            response = reader.country(ip)
            listaPais.append(format(response.country.name))
            #response = reader.city(ip)
            #listaCiudad.append(format(response.city.name))
            listaIp.append(ip)
            listaHora.append(hora)
            listaCorreo.append(correo)
            listaProtocolo.append(protocolo)
    x += 1

#Devolver Scatter con Gatter
ips = comm.gather(listaIp, root=0)
ipsLocal = comm.gather(listaIpLocal, root=0)
paises = comm.gather(listaPais, root=0)
#ciudades = comm.gather(listaCiudad, root=0)
horas = comm.gather(listaHora, root=0)
correos = comm.gather(listaCorreo, root=0)
protocolos = comm.gather(listaProtocolo, root=0)

if(rank ==0):
    organizar(ips, arregloIp, contadorIp)
    organizar(ipsLocal, arregloIpLocal, contadorIpLocal)
    organizar(paises, arregloPais, contadorPais)
    #organizar(ciudades, arregloCiudad, contadorCiudad)
    organizar(horas, arregloHora, contadorHora)
    organizar(correos, arregloCorreo, contadorCorreo)
    organizar(protocolos, arregloProtocolo, contadorProtocolo)
    top20(arregloIp, contadorIp)
    top20(arregloIpLocal, contadorIpLocal)
    top20(arregloPais, contadorPais)
    #top20(arregloCiudad, contadorCiudad)
    top20(arregloHora, contadorHora)
    top20(arregloCorreo, contadorCorreo)
    top20(arregloProtocolo, contadorProtocolo)
    print('----------------- 20 IP -------------')
    resultado(arregloIp,contadorIp)
    print('----------------- 20 IP-Local -------------')
    resultado(arregloIpLocal,contadorIpLocal)
    print('----------------- 20 PAISES -------------')
    resultado(arregloPais,contadorPais)
    #print('----------------- 20 CIUDADES -------------')
    #resultado(arregloCiudad,contadorCiudad)
    print('----------------- 20 HORAS -------------')
    resultado(arregloHora,contadorHora)
    print('----------------- 20 CORREOS -------------')
    resultado(arregloCorreo,contadorCorreo)
    print('----------------- 20 PROTOCOLOS -------------')
    resultado(arregloProtocolo,contadorProtocolo)
