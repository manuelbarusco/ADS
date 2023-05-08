#This file will extract all the useful information from the datasets

import pathlib
import rdflib
from rdflib import Graph
from rdflib.namespace import RDF
import json
import os
import logging




def getLiterals(graph) -> list:
    q = """
    SELECT ?literal { 
        ?s ?p ?literal 
        FILTER isLiteral(?literal)
    }
    """
    match = graph.query(q)

    literals = list()

    for item in match:
        literals.append(str(item[0]))

    return literals


def getClasses(graph) -> list:
    q = """
    SELECT ?class
    WHERE {
        ?s a ?class .
    }
    """
    match = graph.query(q)

    classes = list()

    for item in match:
        classes.append(item[0])

    return classes


def getEntities(graph) -> list:
    q = """
    SELECT ?s
    WHERE {
        ?s a ?class .
    }
    """
    match = graph.query(q)

    entities = list()

    for item in match:
        entities.append(item[0])

    return entities


def getProperties(graph) -> list:
    q = """
    SELECT ?p
    WHERE {
        ?s ?p ?o .
    }
    """
    match = graph.query(q)

    properties = list()

    for item in match:
        properties.append(item[0])

    return properties

def getJenaProblemFiles(f_indexer):
    
    files = list()

    last_dataset = "" 
    while True: 

        line1 = f_indexer.readline()
        if not line1:
            break

        dataset = line1.split("Dataset: ")[1].strip("\n")

        line2 = f_indexer.readline()
        
        file = line2.split("File: ")[1].strip("\n")

        line3 = f_indexer.readline()

        files.append(tuple((dataset, file)))

    return files


def mineFile(datasets_directory_path, dataset, file, f_log):

    dataset_path = datasets_directory_path+"/"+dataset

    file_path = dataset_path+"/"+file

    g = Graph()

    if (os.path.getsize(file_path) / (1024 ** 2)) < 300: 
        try: 
            g.parse(file_path)

            #extract all the info

            classes = getClasses(g)
            entities = getEntities(g)
            literals = getLiterals(g)
            properties = getProperties(g)

            #¢rete a json file for the problem file
            json_file_name = file.replace(".","_")

            print(json_file_name)

            json_file = open(dataset_path+"/"+json_file_name+"-scriptmined.json","w", encoding="utf-8")
            json_dict = {}

            json_dict["classes"] = classes
            json_dict["entities"] = entities
            json_dict["literals"] = literals
            json_dict["properties"] = properties

            json.dump(json_dict, json_file, ensure_ascii=False, indent=4)
        
            del(json_dict)
            json_file.close()
            del(g)
            del(classes)
            del(entities)
            del(literals)
            del(properties)
            

        except rdflib.exceptions.ParserError as e:  
            f_log.write("Dataset: "+dataset+"\nFile: "+file+"\nError: "+str(e)+"\n")
        except Exception as e :
            f_log.write("Dataset: "+dataset+"\nFile: "+file+"\nError: "+str(e)+"\n")
    else :
        f_log.write("Dataset: "+dataset+"\nFile: "+file+"\nError: Bigger than 300MB\n")

def clean():

    datasets_directory_path = "/media/manuel/500GBHDD/Tesi/Datasets"                                #path to the folder of the downloaded datasets
    error_log_file_path = "/home/manuel/Tesi/ACORDAR/Log/miner_error_log.txt"                   #path to the error log file
    indexer_log_file_path ="/home/manuel/Tesi/ACORDAR/Log/indexer_log.txt"                  #path to the indexer error log file

    #open the indexer error log file
    f_indexer = open(indexer_log_file_path, "r")

    #get the files that are not parsed by jena for rdf syntax problems
    files_datasets = getJenaProblemFiles(f_indexer)

    tot = 4734
    i = 1

    for dataset, file in files_datasets:

        #remove the file
        json_file_name = file.replace(".","_")
        if os.path.exists(datasets_directory_path+"/"+dataset+"/"+json_file_name+".json"):
            os.remove(datasets_directory_path+"/"+dataset+"/"+json_file_name+".json")


    f_indexer.close()



def main():
    datasets_directory_path = "/media/manuel/500GBHDD/Tesi/Datasets"                                #path to the folder of the downloaded datasets
    error_log_file_path = "/home/manuel/Tesi/ACORDAR/Log/miner_error_log.txt"                   #path to the error log file
    indexer_log_file_path ="/home/manuel/Tesi/ACORDAR/Log/indexer_log.txt"                  #path to the indexer error log file

    suffixes = [".rdf", ".ttl", ".owl", ".n3", ".nt", ".jsonld", ".nq", ".trig", ".trix"]

    logging.getLogger("rdflib").setLevel(logging.ERROR)

    #open the error log file of the miner
    f_log=open(error_log_file_path, "w+")

    #open the indexer error log file
    f_indexer = open(indexer_log_file_path, "r")

    #get the files that are not parsed by jena for rdf syntax problems
    files_datasets = getJenaProblemFiles(f_indexer)

    tot = 4734
    i = 1

    for dataset, file in files_datasets:

        mineFile(datasets_directory_path, dataset, file, f_log)
        i+=1
        print(str(i)+" over "+str(tot))

    f_log.close()
    f_indexer.close()

if __name__ == "__main__":
    clean() 



    
