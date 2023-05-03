import rdflib
from rdflib import Graph

'''
g = Graph()
g.parse("/media/manuel/500GBHDD/Tesi/Datasets/dataset-50/wappen.rdf")

query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?s
    WHERE {
        ?s a ?c .
    }"""
    
qres = g.query(query)
for row in qres:
    print(row.s)
'''

file = open("/home/manuel/Tesi/ACORDAR/Log/indexer_log.txt", "r")

errors = []

while True:

    line = file.readline()
    
    if not line:
        break

    line1 = file.readline()
    line2 = file.readline()
    
    print("Line1:"+ line)
    print(line1)
    print(line2)

    if ("Dataset" not in line):
        break
        
