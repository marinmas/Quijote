from pyspark import SparkContext
import sys
import string

def word_split(line):
    for c in string.punctuation+ "Â¿!Â«Â»":
        line = line.replace(c,' ')
        line = line.lower()
    return len(line.split())

def main(filename):
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        data = sc.textFile(filename)
        number_words = data.map(word_split)
        total = number_words.sum()
        if filename=='quijote_s05.txt':
            archivo_salida = open('out_quijote_s05.txt', 'w')
            archivo_salida.write(str(total))
        else:
            archivo_salida1=open('out_quijote.txt', 'w')
            archivo_salida1.write(str(total))
            
        
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Uso: python3 {0} <file>")
    else:
        main(sys.argv[1])