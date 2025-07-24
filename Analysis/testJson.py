import json


with open('Analysis/Clustering/Players/archetypeLables.json', 'r') as file:
    data = json.load(file)

print(data['2021']['Guards']['1']['label'])