#Imports
import gzip

#Variables globales
logs = []
nombreArchivos = ['/home/public/201915/muestra1/audit.log.2018-10-03.gz']

#funciones
def llenar_logs():
    f = ''
    global logs
    for x in nombreArchivos:
        f=gzip.GzipFile(fileobj=open(x, 'rb'))
        cadena = f.read()
        logs += cadena.split('\n')
        f.close()
    print(logs[:10])

if __name__ == '__main__':
    llenar_logs()
