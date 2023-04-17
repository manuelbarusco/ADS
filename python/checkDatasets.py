'''
Script for checking the datasets downloaded files

We will:
* clean up the file name from the urls noise 
* check the file extension if present in the cleaned up file name
* assign a file extension to files that has not an extension by using rdfpipe 
* indicate if the datasets are completed or not by considering if all the urls were downloaded
* indicate if the datasets are full parsable or not by considering if all the downloaded files are parsable

In particular we will assign in the dataset.json file, in the \'indexable\' field: 
* 2: dataset that has all the files downloaded and parsable
* 1: dataset that has not all the files downloaded or parsable
* 0: dataset that has no other files than the dataset.json file with the metadata

We will log:
* which datasets are empty: this means datasets that has no other files other the dataset.json file
* which datasets are incomplete: so we can check if the files can be recovered or not 
* which datasets has some zip or archive file inside their folder
'''

import os
import pathlib
import re
import magic
import subprocess
import json
from rdflib import Graph
import rdflib


'''
This function tries to recover a text file (deduced by the mime type) in a ttl file
if it is recoverable, it will recover it and log in the log file 
if it is not recoverable, it will log it and remvoe the attempt

@param folder object folder to the dataset-folder   
@param path to the text file to be recovered
@param file object file to the file to be recovered
@param parsable_files number of parsable files in the dataset folder
@param f_log log file
'''
def recoverFile(folder, path, file, parsable_files, f_log):
    #try to recover to ttl if no extension available with the rdfpipe command
    cmd_str = "rdfpipe "+str(path)+" -o ttl >> "+str(path)+".ttl"
    process = subprocess.run(cmd_str, shell=True)

    if (process.returncode != 0): 
        f_log.write("I tried to recover the file: "+file.name+" in folder: "+folder.name+" from no extension to .ttl extension\n")
        os.remove(str(path)+".ttl")
        parsable_files-=1
    else:
        f_log.write("I recover the file: "+file.name+" in folder: "+folder.name+" from no extension to .ttl extension\n")
        os.remove(path) 

'''
@param dataset_directory path of directory where are all the datasets
@param error_log_file path of the error log file
@param suffixes array of accettable suffixes
@param datasets_indexable_list_file text file with an array with all the datasets id of the content indexable datasets
'''
def startCheck(datasets_directory, error_log_file, suffixes, datasets_indexable_list_file):
    
    #open the error log file
    f_log=open(error_log_file, "w+")

    #open the list of all the content indexable datasets 
    f_indexable=open(datasets_indexable_list_file, "w+")

    for folder in os.scandir(datasets_directory):

        #open the dataset.json file of the dataset
        dataset_json_file = open('dataset.json')
        
        #read the json object inside the file
        dataset_json = json.load(dataset_json_file)    

        parsable_files = dataset_json["download_info"]["downloaded"]
        
        for file in os.scandir(folder):

            #not consider the dataset.json file

            if file.name != "dataset.json":

                path = pathlib.Path(file)

                #clean up the file name and highlight the file extension: check for REST API syntax

                match = re.search("\.*[?\/_#:]", file.name)

                if(match):
                    sub_string = file.name[0:match.start()]
                    os.rename(path, datasets_directory+"/"+folder.name+"/"+sub_string)
                    path = datasets_directory+"/"+folder.name+"/"+sub_string

                #check for the file extension (file suffix)

                file_suffix = pathlib.Path(path).suffix

                if (file_suffix not in suffixes):

                    #get file and mime types

                    file_type = magic.from_file(path)

                    mime_type = magic.from_file(path, mime=True)

                    #recover from file without extension

                    if "xml" in mime_type:
                        os.rename(path, str(path)+".rdf")
                        f_log.write("The file: "+file.name+" in folder: "+folder.name+" probably has a XML/RDF valid extension: "+file_type+" so I recovered it\n")
                        
                        #check it the new file is parsable
                        g = Graph()
                        
                        try: 
                            g.parse(str(path)+".rdf")
                        except rdflib.exceptions.ParserError as e: 
                            f_log.write("The recoverd file: "+str(path)+" cannot be parsed from rdflib\n")
                            parsable_files -= 1

                    elif "text" in mime_type:
                        recoverFile(folder, path, file, parsable_files, f_log)

                    else : 
                        print("The file: "+file.name+" in folder: "+folder.name+" has not a RDF valid extension: "+file_suffix+"\n")
                        f_log.write("The file: "+file.name+" in folder: "+folder.name+" has not a RDF valid extension: "+file_suffix+"\nFile type: "+file_type+"\n")
                        parsable_files -= 1

        if dataset_json["download_info"]["downloaded"] == 0:
            dataset_json["indexable"] = 0
    
        if parsable_files < dataset_json["download_info"]["downloaded"]:
            dataset_json["indexable"] = 1

        if parsable_files == dataset_json["download_info"]["downloaded"]:
            dataset_json["indexable"] = 2

        #if the dataset is indexable add its id to the list file
        if dataset_json["indexable"] == 1 or dataset_json["indexable"] == 2:
            f_indexable.write(dataset_json["dataset_id"])

        del(dataset_json)
        dataset_json_file.close()

    f_log.close()
    f_indexable.close()

def main():
    #datasets_directory = "/home/manuel/Tesi/ACORDAR/Test" 
    datasets_directory = "/media/manuel/500GBHDD/Tesi/Datasets"                                   #path to the folder of the downloaded datasets
    error_log_file = "/home/manuel/Tesi/ACORDAR/Log/checker_error_log.txt"                        #path to the error log file
    datasets_indexable_list_file = "/home/manuel/Tesi/ACORDAR/Data/indexable_datasets.txt"   #path to the list file with all the full indexable datasets

    suffixes = [".rdf", ".ttl", ".owl", ".n3", ".nt", ".jsonld", ".xml"]

    startCheck(datasets_directory, error_log_file, suffixes, datasets_indexable_list_file)
