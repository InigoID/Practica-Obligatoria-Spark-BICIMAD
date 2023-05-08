from pyspark import SparkContext
import json


def map_line_to_dict(line):
    dic = {}
    data = json.loads(line)
    dic['usuario'] = data['user_day_code']
    dic['user_type'] = data['user_type']
    dic['start'] = data['idunplug_station']
    dic['end'] = data['idplug_station']
    time = data['unplug_hourTime']['$date']
    fecha, hora = time.split('T')
    año, mes, dia = fecha.split('-')
    hora, *s = hora.split(':')
    dic['año'] = int(año)
    dic['mes'] = int(mes)
    dic['dia'] = int(dia)
    dic['hora'] = int(hora)
    dic['code'] = data['zip_code']
    return dic


class BiciMadAnalysis:
    def __init__(self, file_paths):
        self.sc = SparkContext()
        self.rdd_base = self.sc.textFile(",".join(file_paths))

    def group_by_key_count(self, data, key):
        grouped_data = data.map(lambda x: (x[key], 1)).groupByKey().mapValues(len).collect()
        grouped_data = sorted([(x[1], x[0]) for x in grouped_data], reverse=True)
        grouped_data = [(x[1], x[0]) for x in grouped_data]
        return grouped_data

    def analyze_data(self):
        rdd = self.rdd_base.map(map_line_to_dict)
        morning_data = rdd.filter(lambda x: 6 <= x['hora'] < 14)
        afternoon_data = rdd.filter(lambda x: 14 <= x['hora'] < 22)
        night_data = rdd.filter(lambda x: 6 > x['hora'] or x['hora'] >= 22)
        morning_stations = self.group_by_key_count(morning_data, 'start')
        afternoon_stations = self.group_by_key_count(afternoon_data, 'start')
        night_stations = self.group_by_key_count(night_data, 'start')
        print(f"La estacion mas usada por la mañana es {morning_stations[0]}")
        print(f"La estacion mas usada por la tarde es {afternoon_stations[0]}")
        print(f"La estacion mas usada por la noche es {night_stations[0]}")
        morning_stations_least = morning_stations[-1]
        afternoon_stations_least = afternoon_stations[-1]
        night_stations_least = night_stations[-1]
        print(f"La estacion menos usada por la mañana es {morning_stations_least}")
        print(f"La estacion menos usada por la tarde es {afternoon_stations_least}")
        print(f"La estacion menos usada por la noche es {night_stations_least}")
        zip_data = self.group_by_key_count(rdd, 'code')
        print("Top 5 codigos postales con mas viajes:")
        for i in range(5):
            print(f"{i + 1}. CP {zip_data[i][0]} con {zip_data[i][1]} viajes")


if __name__ == "__main__":
    files = ["abril2017.json", "mayo2017.json", "junio2017.json", "julio2017.json", "agosto2017.json", "septiembre2017.json", "octubre2017.json", "noviembre2017.json", "diciembre2017.json"]
    analysis = BiciMadAnalysis(files)
    analysis.analyze_data()
