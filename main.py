#Imports




#Variables Globales
logs = []
nombreArchivos = ['audit.log.2018-10-03']

#Funciones
def llenar_logs():
    cadena = ''
    for x in nombreArchivos:
        with open(x) as archivo:
            cadena = archivo.read()
            logs += cadena.split('\n')
    print(logs[0:10])

if __name__ == "__main__":
    llenar_logs()
