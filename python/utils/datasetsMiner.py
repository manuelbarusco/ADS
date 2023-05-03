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
    SELECT ?b
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
def extractClassesWithRank(g,json_dict):
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
    SELECT ?s
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
    SELECT ?l
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
    SELECT ?p
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

        #read the dataset.json file previously prodcued by the downloader

        dataset_json_file = open(folder.path+"/dataset.json", "r")
        dataset_json = json.load(dataset_json_file,strict=False)
        dataset_json_file.close()

        #resume mechanism: check the mined = true field in the dataset.json

        if not dataset_json["mined"]:

            for file in os.scandir(folder):

                #consider only files with the correct extension

                if pathlib.Path(file.path).suffix in suffixes:

                    print("Considering: "+file.path)

                    try:
                        g = Graph()
                        g.parse(file.path)

                        extractClasses(g, dataset_json)
                        extractLiterals(g,dataset_json)
                        extractProperties(g,dataset_json)
                        extractEntities(g,dataset_json)

                        #free memory            
                        del g
                    except Exception as e: 
                        print("Failing parsing file: "+file.path)
                        f_log.write("Mining Dataset: "+folder.name+"\nFailing parsing file: "+file.path)

            # Set mined to true for indicating that the dataset is mined
            dataset_json["mined"] = True

            # Serializing json_dict
            json_dict_object = json.dumps(dataset_json, indent=4)

            # Writing to sample.json_dict
            outfile = open(folder.path+"/dataset.json", "w")
            outfile.write(json_dict_object)

            #free memory 
            del json_dict_object
            del dataset_json
            outfile.close()

            print("End mining "+folder.name)

    f_log.close()

if __name__ == "__main__":
    main() 



    
