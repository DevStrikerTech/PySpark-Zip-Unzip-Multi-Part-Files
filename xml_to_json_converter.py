import json
import xml.etree.ElementTree as ET

tree = ET.parse('input.xml')
root = tree.getroot()


def converter(root):
    response = {}
    obj = []
    countries = []
    neighbors = []

    country = [i.tag for i in root]
    neighbor = [j.tag for j in root.iter('neighbor')]

    for items_2 in root.iter('neighbor'):
        neighbors.append(items_2.attrib)

    for items in root.findall('country'):
        countries.append({
            'rank': items.find('rank').text,
            'year': items.find('year').text,
            neighbor[0]: neighbors
        })

    obj.append({country[0]: countries})

    response[root.tag] = obj

    return response


def write_to_json_file():
    with open('output.json', 'w') as create_json_file:
        json.dump(converter(root), create_json_file, indent=3)


if __name__ == '__main__':
    write_to_json_file()
