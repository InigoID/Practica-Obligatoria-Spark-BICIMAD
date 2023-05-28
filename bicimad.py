# imports
from pyspark import SparkContext
import json
import matplotlib.pyplot as plt

# función que convierte cada línea de los datos en un diccionario
def map_line_to_dict(line):
    dic = {}
    data = json.loads(line)
    dic['usuario'] = data['user_day_code']
    dic['tipo_usuario'] = data['user_type']
    dic['inicio'] = data['idunplug_station']
    dic['fin'] = data['idplug_station']
    tiempo = data['unplug_hourTime']['$date']
    fecha, hora = tiempo.split('T')
    año, mes, dia = fecha.split('-')
    hora, *s = hora.split(':')
    dic['año'] = int(año)
    dic['mes'] = int(mes)
    dic['dia'] = int(dia)
    dic['hora'] = int(hora)
    dic['codigo'] = data['zip_code'] if data['zip_code'] else "Desconocido"
    return dic

# clase que lee y analiza los datos
class AnalisisBiciMad:
	# función de inicialización
    def __init__(self, rutas_archivos):
        self.sc = SparkContext()
        self.rdd_base = self.sc.textFile(",".join(rutas_archivos))
	# función que agrupa los datos y cuenta cuántos registros hay para la clave
    def group_by_key_count(self, data, clave):
        datos_agrupados = data.map(lambda x: (x[clave], 1)).groupByKey().mapValues(len).collect()
        datos_agrupados = sorted([(x[1], x[0]) for x in datos_agrupados], reverse=True)
        datos_agrupados = [(x[1], x[0]) for x in datos_agrupados]
        return datos_agrupados
	# función analiza los datos y hace una gráfica
    def analizar_datos(self):
    	# convertir a diccionario
        rdd = self.rdd_base.map(map_line_to_dict)
        # franjas horarias
        datos_mañana = rdd.filter(lambda x: 6 <= x['hora'] < 14)
        datos_tarde = rdd.filter(lambda x: 14 <= x['hora'] < 22)
        datos_noche = rdd.filter(lambda x: 6 > x['hora'] or x['hora'] >= 22)
        estaciones_mañana = self.group_by_key_count(datos_mañana, 'inicio')
        estaciones_tarde = self.group_by_key_count(datos_tarde, 'inicio')
        estaciones_noche = self.group_by_key_count(datos_noche, 'inicio')
        print(f"La estación más usada por la mañana es la estación número {estaciones_mañana[0][0]} con {estaciones_mañana[0][1]} viajes.")
        print(f"La estación más usada por la tarde es la estación número {estaciones_tarde[0][0]} con {estaciones_tarde[0][1]} viajes.")
        print(f"La estación más usada por la noche es la estación número {estaciones_noche[0][0]} con {estaciones_noche[0][1]} viajes.")
        print(f"La estación menos usada por la mañana es la estación número {estaciones_mañana[-1][0]} con {estaciones_mañana[-1][1]} viajes.")
        print(f"La estación menos usada por la tarde es la estación número {estaciones_tarde[-1][0]} con {estaciones_tarde[-1][1]} viajes.")
        print(f"La estación menos usada por la noche es la estación número {estaciones_noche[-1][0]} con {estaciones_noche[-1][1]} viajes.")
        # códigos postales
        datos_cp = self.group_by_key_count(rdd, 'codigo')
        print("Top 5 códigos postales con más viajes:")
        for i in range(5):
            print(f"{i + 1}. CP {datos_cp[i][0]} con {datos_cp[i][1]} viajes")
		#estaciones del año para hacer gráfica de uso en los top 5 códigos postales
        estaciones = ['invierno', 'primavera', 'verano', 'otoño']
        meses_por_estacion = [('invierno', [12, 1, 2]), ('primavera', [3, 4, 5]), ('verano', [6, 7, 8]), ('otoño', [9, 10, 11])]
        datos_cp_por_estacion = {estacion: self.group_by_key_count(rdd.filter(lambda x: x['mes'] in meses), 'codigo') for estacion, meses in meses_por_estacion}
        top_5_codigos_postales = [codigo_postal for codigo_postal, _ in datos_cp[:5]]
        x = range(len(estaciones))
        for codigo_postal in top_5_codigos_postales:
            conteos = []
            for estacion in estaciones:
                datos_estacion = datos_cp_por_estacion[estacion]
                conteo = next((conteo for codigo_postal2, conteo in datos_estacion if codigo_postal2 == codigo_postal), 0)
                conteos.append(conteo)
            plt.plot(x, conteos, label=f'CP {codigo_postal}')
        plt.xticks(x, estaciones)
        plt.legend()
        plt.title('Número de viajes por estación y código postal (Top 5 códigos postales)')
        plt.xlabel('Estación')
        plt.ylabel('Número de viajes')
        plt.show()

# inicialización de clase con los archivos de abril 2017 a marzo 2018
if __name__ == "__main__":
    archivos = ["abril2017.json", "mayo2017.json", "junio2017.json", "julio2017.json", "agosto2017.json", "septiembre2017.json", "octubre2017.json", "noviembre2017.json", "diciembre2017.json", "enero2018.json","febrero2018.json","marzo2018.json"]
    analisis = AnalisisBiciMad(archivos)
    analisis.analizar_datos()
