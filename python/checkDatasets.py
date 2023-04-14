#script for checking the downloaded files

import os
import pathlib
import re
import magic
import subprocess

#datasets_directory = "/home/manuel/Tesi/ACORDAR/Test" 
datasets_directory = "/media/manuel/500GBHDD/Tesi/Datasets"                              #path to the folder of the downloaded datasets
error_log_file = "/home/manuel/Tesi/ACORDAR/Log/checker_error_log.txt"                   #path to the error log file

suffixes = [".rdf", ".ttl", ".owl", ".n3", ".nt", ".jsonld", ".xml"]

#open the error log file
f_log=open(error_log_file, "w+")

i=0

for folder in os.scandir(datasets_directory):

    for file in os.scandir(folder):

        #not consider the dataset.json file

        if file.name != "dataset.json":

            path = pathlib.Path(file)

            #clean the file name and highlight the file extension: check for REST API syntax

            match = re.search("\.*[?\/_]", file.name)

            if(match):
                sub_string = file.name[0:match.start()]
                os.rename(path, datasets_directory+"/"+folder.name+"/"+sub_string)
                path = datasets_directory+"/"+folder.name+"/"+sub_string

            #check for the suffix

            file_suffix = pathlib.Path(path).suffix

            if (file_suffix not in suffixes):

                #get file and mime types

                file_type = magic.from_file(path)

                mime_type = magic.from_file(path, mime=True)

                #recover from file without extension

                if "xml" in mime_type:
                    os.rename(path, str(path)+".rdf")
                    f_log.write("The file: "+file.name+" in folder: "+folder.name+" probably has a XML/RDF valid extension: "+file_type+" so I recovered it\n")
                
                elif "text" in mime_type:

                    #try to recover to ttl if no extension available with the rdfpipe command
                    cmd_str = "rdfpipe "+str(path)+" -o ttl >> "+str(path)+".ttl"
                    process = subprocess.run(cmd_str, shell=True)
                    if (process.returncode != 0): 
                        f_log.write("I tried to recover the file: "+file.name+" in folder: "+folder.name+" from no extension to .ttl extension\n")
                        os.remove(str(path)+".ttl")
                    else:
                        f_log.write("I recover the file: "+file.name+" in folder: "+folder.name+" from no extension to .ttl extension\n")
                        os.remove(path) 

                else : 
                    print("The file: "+file.name+" in folder: "+folder.name+" has not a RDF valid extension: "+file_suffix+"\n")
                    f_log.write("The file: "+file.name+" in folder: "+folder.name+" has not a RDF valid extension: "+file_suffix+"\nFile type: "+file_type+"\n")

            i+=1

    if i == 0: 
        print("The directory: "+folder.name+" contains only the dataset.json file\n")
        f_log.write("The directory: "+folder.name+" contains only the dataset.json file\n")

f_log.close()
