# Grupo 11: Íñigo Iriondo, Álvaro Pleguezuelos y Alejandro Hernando

Definición del problema y motivación

El objetivo de esta práctica es estudiar el uso del sistema de bicicletas de préstamo BICIMAD en Madrid, viendo las estaciones más y menos utilizadas en diferentes horarios del día y los códigos postales más populares.


Diseño e implementación de la solución

Utilizamos PySpark para analizar los datos de BICIMAD del ayuntamiento de Madrid desde abril hasta diciembre de 2017. La implementación consiste en:
Importación de SparkContext y json.
Definición de map_line_to_dict que pasa cada línea de datos a un diccionario.
Creación de la clase BiciMadAnalysis que, en el contexto Spark, lee los archivos de datos de BICIMAD y los analiza: primero, la función group_by_key_count agrupa los datos y cuenta los respectivos registros, y después, analyze_data los separa en mañana (6h-14h), tarde (14h-22h) y noche (22h-6h), identifica las estaciones más y menos usadas en cada horario y los 5 códigos postales con más viajes realizados utilizando group_by_key_count y finalmente, imprime los resultados.
Inicialización de la clase que llama a analyze_data con el input de los archivos.


Evaluación de resultados y aplicación de la solución al conjunto de datos

La solución se aplica a los datos de BICIMAD desde abril hasta diciembre de 2017. Obtenemos los siguientes resultados:

La estación más usada por la mañana es (163, 13782)                             
La estación más usada por la tarde es (43, 21387)
La estación más usada por la noche es (57, 10756)
La estación menos usada por la mañana es (2008, 30)
La estación menos usada por la tarde es (2008, 11)
La estación menos usada por la noche es (119, 465)

Top 5 códigos postales con más viajes:                                          
1. CP  con 966638 viajes
2. CP 28005 con 203757 viajes
3. CP 28012 con 189331 viajes
4. CP 28004 con 168275 viajes
5. CP 28007 con 164698 viajes
