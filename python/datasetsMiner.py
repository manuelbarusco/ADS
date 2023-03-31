#This file will extract all the useful information from the datasets

import pathlib
from rdflib import Graph
from rdflib.namespace import RDF
import json
import os

'''
FOR FUTURE PURPOSE 

def extractClasses(g):
    query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT DISTINCT ?b
    WHERE {
        ?a a ?b .
    }"""
    
    classes = []

    qres = g.query(query)
    for row in qres:
        i = len(row.b)
        while i > 0:
            i -= 1
            if(row.b[i]=="#" or row.b[i]=="/"):
                break 
        classes.append(row.b[i+1:len(row.b)])
    
    return classes
'''

'''
This method will extract all the classes
@param g graph where to extract
@param json_dict json dictionary where to save the list
'''
def extractClasses(g,json_dict):
    query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT DISTINCT ?c
    WHERE {
        ?s a ?c .
    }"""

    qres = g.query(query)
    classes = []
    for row in qres:
        classes.append(row.c)
    
    if "classes" in json_dict:
        json_dict["classes"].extend(classes)
    else: 
        json_dict["classes"]=classes


'''
This method will extract all the entities
@param g graph where to extract
@param json_dict json dictionary where to save the list
'''
def extractEntities(g,json_dict):
    query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT DISTINCT ?s
    WHERE {
        ?s a ?c .
    }"""
    
    entities = []
    qres = g.query(query)
    for row in qres:
        entities.append(row.s)
    
    if "entities" in json_dict:
        json_dict["entities"].extend(entities)
    else: 
        json_dict["entities"]=entities

    

'''
This method will extract all the literals
@param g graph where to extract
@param json_dict json dictionary where to save the list
'''
def extractLiterals(g,json_dict):
    query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT DISTINCT ?l
    WHERE {
        ?s ?p ?l .
        FILTER(isLiteral(?l))
    }"""
    
    literals = []
    qres = g.query(query)
    for row in qres:
        literals.append(row.l)
    
    if "literals" in json_dict:
        json_dict["literals"].extend(literals)
    else: 
        json_dict["literals"]=literals


'''
This method will extract all the properties
@param g graph where to extract
@param json_dict json dictionary where to save the list
'''
def extractProperties(g,json_dict):
    query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT DISTINCT ?p
    WHERE {
        ?s ?p ?o .
    }"""
    
    properties = []
    qres = g.query(query)
    for row in qres:
        properties.append(row.p)
    
    if "properties" in json_dict:
        json_dict["properties"].extend(properties)
    else: 
        json_dict["properties"]=properties


def main():
    datasets_directory_path = "/home/manuel/Tesi/ACORDAR/Test"                             #path to the folder of the downloaded datasets
    error_log_file = "/home/manuel/Tesi/ACORDAR/Log/miner_error_log.txt"                   #path to the error log file

    suffixes = [".rdf", ".ttl", ".owl", ".n3", ".nt"]

    #open the error log file
    f_log=open(error_log_file, "w+")

    for folder in os.scandir(datasets_directory_path):

        print("Start mining "+folder.name)

        json_dict = {}

        for file in os.scandir(folder):

            #consider only files with the correct extension

            if pathlib.Path(file.path).suffix in suffixes:

                print("Considering: "+file.path)

                try:
                    g = Graph()
                    g.parse(file.path)

                    extractClasses(g, json_dict)
                    extractLiterals(g,json_dict)
                    extractProperties(g,json_dict)
                    extractEntities(g,json_dict)

                    #free memory            
                    del g
                except Exception as e: 
                    print("Failing parsing file: "+file.path)
                    f_log.write("Mining Dataset: "+folder.name+"\nFailing parsing file: "+file.path)

        # Serializing json_dict
        json_dict_object = json.dumps(json_dict, indent=4)
        # Writing to sample.json_dict
        with open(folder.path+"/dataset.json", "w") as outfile:
            outfile.write(json_dict_object)

        #free memory 
        del json_dict_object
        del json_dict
        outfile.close()
        f_log.close()

        print("End mining "+folder.name)

if __name__ == "__main__":
    main() 



    
