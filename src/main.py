# This is a sample Python script.

# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

# Este proyecto es para aprender a usar ApacheBeam ( Dataflow ).
# Leerá un fichero de entrada y generará un fichero de salida
# Se podrá ejecutar tanto en local como en gcP


import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def main():
    parser = argparse.ArgumentParser(description="Nuestro primer PipeLine en DataFlow")
    parser.add_argument("--entrada", help="Fichero de entrada")
    parser.add_argument("--salida", help = "Fichero de salida resultado")

    our_args, beam_args = parser.parse_known_args()
    run_pipeline(our_args, beam_args)


def run_pipeline(custom_args, beam_args):
    entrada = custom_args.entrada
    salida = custom_args.salida

    # Esto le dice a ApacheBeam donde tiene que ejecutar el PipeLine, si en local con estas opciones, en Cloud con estas opciones, etc
    opts=PipelineOptions(beam_args)

    # No es obligatorio usar with, lo que asi nos aseguramos que se ejecutara el PipeLine cuando acabemos de ejecutarlo
    # Cuando se salga del contexto del with se ejecutara el PipeLine
    # Es la manera recomendada de ejecutar pipeline en python

    #IMPORTANTE: Las operaciones en ApacheBeam se concatenan con el pipe ( | ). Realmente un Pipeline es como
    # la concatenacion de comandos en la Shell de Unix

    # El modulo leera el fichero linea por linea y en cada uno de de los elementos en la coleccion "lineas"
    # "lineas" es una coleccion, no es una lista de python, no se puede acceder al elemento 7 porque no existe el elemento 7
    # Es una coleccion distribuida. Hay elementos distribuidos en un cluster de procesado paralelo, no ha un orden, segun se lean y se generen
    # aparecera antes o despues.
    with beam.Pipeline(options = opts) as p:

        # Esto es una PCollection
        # En lineas tendremos algo como "En un lugar de la mancha" y queremos "En" "un" "lugar" "de" "la" "mancha"
        lineas = p | beam.io.ReadFromText(entrada)

        # Con esto lo que queremos es tener una lsita por cada linea
        # Cada lista tendra ["En", "un", "lugar", "de", "la", "mancha"]
        # Cuando lea otra linea del tipo "de cuyo nombre no quiero acordarme" tendra otra lista que sera ["de", "cuyo", "nombre"...]
        palabras = lineas | beam.FlatMap(lambda l: l.split())  # Flat map siempre admite una funcion como entrada

        # Como queremos una coleecion con todos los elementos de todas las listas generaremos un FlatMap
        #partido | beam.FlatMap(lambda l: l)  --> En lugar de pasar la funcion identidad aplicamos el FlatMap arriba

        # En el clasico ejemplo de contar palabras, en cualquier Framework de BigData lo que se genera es
        # "En" --> ("En", 1)
        # "un" --> ("un", 1)
        # "En" --> ("En", 1)
        # Y luego lo que hace es agrupar y suma, PERO EN APACHE BEAM NO HACE FALTA, YA ESETA PREPARADO
        # TIENE FUNCIONES CONVINADORES PARA CONTAR ELEMENTOS

        contadas = palabras | beam.combiners.Count.PerElement() # Esto devolvera Tuplas ("En", 19), ("un", 50)

        # Le estoy pasando por tuplas, para el orden me interesa ordenar por el valor
        palabras_top_lista = contadas | beam.combiners.Top.Of(5, key=lambda kv: kv[1]) # [('de', 2), ('el', 2), ('la', 1), ('un', 1), ('lugar', 1)]
        palabra_top_sin_lista = palabras_top_lista | beam.FlatMap((lambda x:x)) # Elimino que o tenga lista
        formateado = palabra_top_sin_lista | beam.Map(lambda kv: "%s,%d"  %(kv[0], kv[1]))
        #palabras_top_lista | beam.Map(print)
        print("ahora el formateado")
        formateado | beam.Map(print)
        #print (palabras_top)
        formateado | beam.io.WriteToText(salida)


        # Se escribe la salida a un fichero









# Press the green button in the gutter to run the script.
if __name__ == '__main__':
  main()


