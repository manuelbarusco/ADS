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
def recoverTextFile(folder, path, file, parsable_files, f_log):
    #try to recover to ttl if no extension available with the rdfpipe command
    cmd_str = "rdfpipe "+str(path)+" -o ttl >> "+str(path)+".ttl"
    process = subprocess.run(cmd_str, shell=True)

    print(str(process.returncode))

    if (process.returncode != 0): 
        f_log.write("I tried to recover the file: "+file.name+" in folder: "+folder.name+" from no extension to .ttl extension\n")
        os.remove(str(path)+".ttl")
        parsable_files-=1
    else:
        f_log.write("I recover the file: "+file.name+" in folder: "+folder.name+" from no extension to .ttl extension\n")
        os.remove(path) 
    return parsable_files

'''
@param dataset_directory path of directory where are all the datasets
@param error_log_file path of the error log file
@param suffixes array of accettable suffixes
'''
def startCheck(datasets_directory, error_log_file, suffixes):
    
    #open the error log files
    f_log=open(error_log_file, "a")

    index = 0
    dataset_id = 0

    for folder in os.scandir(datasets_directory):

        if index % 1000 == 0:
            print("Checked: "+str(index)+" datasets")

        dataset_path = datasets_directory+"/"+folder.name

        #open the dataset.json file of the dataset
        dataset_json_file = open(dataset_path+"/dataset.json", "r")
        
        #read the json object inside the file
        dataset_json = json.load(dataset_json_file, strict=False)    

        dataset_json_file.close()

        #check if the dataset was already checked with the presence of the "indexable" field in the dataset.json
        if "indexable" not in dataset_json: 

            dataset_json_file = open(dataset_path+"/dataset.json", "w")

            #assume that the downloaded files are parsable, we will remove 1 from the this counter
            #for every not parsable file
            parsable_files = dataset_json["download_info"]["downloaded"]
            
            for file in os.scandir(folder):

                #not consider the dataset.json file

                if file.name != "dataset.json":

                    path = pathlib.Path(file)


                    match = re.search("\.*[?\/_#:]", file.name)

                    if(match):
                        sub_string = file.name[0:match.start()]
                        os.rename(path, datasets_directory+"/"+folder.name+"/"+sub_string)
                        path = dataset_path+"/"+sub_string

                    #check for the file extension (file suffix)

                    file_suffix = pathlib.Path(path).suffix

                    if (file_suffix not in suffixes):

                        #get file and mime types

                        file_type = magic.from_file(path)

                        mime_type = magic.from_file(path, mime=True)

                        #recover from file without extension

                        if "xml" in mime_type:
                            #the xml mime type many times is a rdf file

                            os.rename(path, str(path)+".rdf")
                            f_log.write("The file: "+file.name+" in folder: "+folder.name+" probably has a XML/RDF valid extension: "+file_type+" so I recovered it\n")
                            
                            #check it the new file is parsable with rdflib
                            g = Graph()
                            
                            try: 
                                g.parse(str(path)+".rdf")
                            except rdflib.exceptions.ParserError as e: 
                                f_log.write("The recoverd file: "+str(path)+" cannot be parsed from rdflib\n")
                                parsable_files -= 1

                        elif "text" in mime_type:
                            parsable_files = recoverTextFile(folder, path, file, parsable_files, f_log)

                        else : 
                            print("The file: "+file.name+" in folder: "+folder.name+" has not a RDF valid extension: "+file_suffix+"\n")
                            f_log.write("The file: "+file.name+" in folder: "+folder.name+" has not a RDF valid extension: "+file_suffix+"\nFile type: "+file_type+"\n")
                            parsable_files -= 1

            if dataset_json["download_info"]["downloaded"] == 0 or parsable_files == 0:
                dataset_json["indexable"] = 0
        
            if parsable_files > 0 and parsable_files < dataset_json["download_info"]["downloaded"] :
                dataset_json["indexable"] = 1

            if parsable_files == dataset_json["download_info"]["downloaded"]:
                dataset_json["indexable"] = 2

            #if the dataset is indexable add its id to the list file
            if dataset_json["indexable"] == 1 or dataset_json["indexable"] == 2:
                f_indexable.write(dataset_json["dataset_id"])

            json_dict_object = json.dumps(dataset_json, indent=4)
            dataset_json_file.write(json_dict_object)

            del(dataset_json)
            del(json_dict_object)
            dataset_json_file.close()
        
        index += 1


    f_log.close()
    f_indexable.close()

def main():
    #datasets_directory = "/home/manuel/Tesi/ACORDAR/Test" 
    datasets_directory = "/media/manuel/500GBHDD/Tesi/Datasets"                                   #path to the folder of the downloaded datasets
    error_log_file = "/home/manuel/Tesi/ACORDAR/Log/checker_error_log.txt"                        #path to the error log file

    suffixes = [".rdf", ".ttl", ".owl", ".n3", ".nt", ".jsonld", ".xml"]

    startCheck(datasets_directory, error_log_file, suffixes)

if __name__ == "__main__" :
    main()