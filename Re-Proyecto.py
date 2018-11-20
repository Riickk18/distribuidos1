#!/usr/bin/env python
from mpi4py import MPI
from geoip2.errors import AddressNotFoundError
import geoip2.database
#import pygeoip

#Inicializar el COMM_WORLD y sus mecanismos
comm = MPI.COMM_WORLD
rank = comm.rank
size = comm.Get_size()
name = MPI.Get_processor_name()

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

fragmento = 0

#Verifica cuantos procesos son y crear Arreglos necesarios
def picar(size):
    global fragmento
    x = 0
    while(x<size):
        arregloPrincipal.append([])
        x += 1
    fragmento = size-1

# #Leemos los Logs e ingresa en el arreglo
# def leerLog(archivo, longitud):
#     global fragmento
#     x,y = 0,0
#     while(x<longitud):
#         if(y>fragmento):
#             y = 0
#         txt = archivo.readline()
#         if 'WARN' in txt:
#             arregloPrincipal[y].append(txt)
#             y += 1
#             x += 1

def leerLog(archivo):
    cadena = archivo.read()
    arregloLogs = cadena.split('\n')
    arregloWarns = []
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
        x+=1

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
        print(str(x+1) + ". ---- " + principal[x] + " ---- Ataques: " + str(contador[x]) + " ---- Porcentaje: " + str(p) + "%")
        x += 1

################################### M P I ########################################
if(rank == 0):
    picar(size)
    archivo = open("audit.log.2018-10-04", "r")
    leerLog(archivo)
    print('Termine de leer Logs')
    archivo.close()
else:
    arregloPrincipal = None

print('\n Empezando el Scatter')
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
            reader = geoip2.database.Reader('/home/group/distribuidos/201915_15579/10/workspace/GeoLite2-City_20181113/GeoLite2-City.mmdb')
            response = reader.city(ip)
            listaPais.append(format(response.country.name))
            listaCiudad.append(format(response.city.name))
            listaIp.append(ip)
            listaHora.append(hora)
            listaCorreo.append(correo)
            listaProtocolo.append(protocolo)
    x += 1
print('Termine, soy el proceso: ' +str(rank)+ ' del nodo: '+name)

#Devolver Scatter con Gatter
ips = comm.gather(listaIp, root=0)
ipsLocal = comm.gather(listaIpLocal, root=0)
paises = comm.gather(listaPais, root=0)
ciudades = comm.gather(listaCiudad, root=0)
horas = comm.gather(listaHora, root=0)
correos = comm.gather(listaCorreo, root=0)
protocolos = comm.gather(listaProtocolo, root=0)

if(rank ==0):
    organizar(ips, arregloIp, contadorIp)
    organizar(ipsLocal, arregloIpLocal, contadorIpLocal)
    organizar(paises, arregloPais, contadorPais)
    organizar(ciudades, arregloCiudad, contadorCiudad)
    organizar(horas, arregloHora, contadorHora)
    organizar(correos, arregloCorreo, contadorCorreo)
    organizar(protocolos, arregloProtocolo, contadorProtocolo)
    top20(arregloIp, contadorIp)
    top20(arregloIpLocal, contadorIpLocal)
    top20(arregloPais, contadorPais)
    top20(arregloCiudad, contadorCiudad)
    top20(arregloHora, contadorHora)
    top20(arregloCorreo, contadorCorreo)
    top20(arregloProtocolo, contadorProtocolo)
    print('-------------------------------------------------------------------------------------------------')
    print('--------------------------------- Direcciones IP Que Màs Atacan ---------------------------------')
    print('-------------------------------------------------------------------------------------------------')
    resultado(arregloIp,contadorIp)
    #print('----------------- 20 IP-Local -------------')
    #resultado(arregloIpLocal,contadorIpLocal)
    print('-------------------------------------------------------------------------------------------------')
    print('------------------------------------- Paises Que Màs Atacan -------------------------------------')
    print('-------------------------------------------------------------------------------------------------')
    resultado(arregloPais,contadorPais)
    print('-------------------------------------------------------------------------------------------------')
    print('------------------------------------ Ciudades Que Màs Atacan ------------------------------------')
    print('-------------------------------------------------------------------------------------------------')
    resultado(arregloCiudad,contadorCiudad)
    print('-------------------------------------------------------------------------------------------------')
    print('------------------------------------ Horas En Que Màs Atacan ------------------------------------')
    print('-------------------------------------------------------------------------------------------------')
    resultado(arregloHora,contadorHora)
    print('-------------------------------------------------------------------------------------------------')
    print('------------------------------ Correos Electrònicos Que Màs Atacan ------------------------------')
    print('-------------------------------------------------------------------------------------------------')
    resultado(arregloCorreo,contadorCorreo)
    print('-------------------------------------------------------------------------------------------------')
    print('-------------------------------------- Protocolos De Ataque -------------------------------------')
    print('-------------------------------------------------------------------------------------------------')
    resultado(arregloProtocolo,contadorProtocolo)
