import os
import json

config_path = os.path.join(os.path.dirname(__file__), 'provinces.json')


def read_provinces():
    with open(config_path, 'r', encoding='UTF-8') as file:
        result = []
        provinces = json.load(file)
        for province in provinces:
            province_id = province.get('id')
            province_cities = province.get('cities')
            if province_id and province_cities:
                for city in province_cities:
                    city_id = city.get('id')
                    if city_id:
                        result.append('%s:%s' % (province_id, city_id))
    return result
