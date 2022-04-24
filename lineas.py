import random

archivo_entrada = open('quijote.txt', 'r')
archivo_salida = open('quijote_s05.txt', 'w')
p = random.random()
for linea in archivo_entrada:
    d = random.random()
    if d <= p:
        archivo_salida.write(linea)
archivo_entrada.close()
archivo_salida.close()