#script for checking the downloaded files

import os
import pathlib
import re
import magic

datasets_directory = "/home/manuel/Tesi/ACORDAR/Test"                                    #path to the folder of the downloaded datasets
error_log_file = "/home/manuel/Tesi/ACORDAR/Log/checker_error_log.txt"                   #path to the error log file

suffixes = [".rdf", ".ttl", ".owl", ".n3", ".nt"]

#open the error log file
f_log=open(error_log_file, "w+")

for folder in os.scandir(datasets_directory):

    i = 0

    for file in os.scandir(folder):

        #not consider the dataset.json file

        if file.name != "dataset.json":

            path = pathlib.Path(file)

            #clean the file name and highlight the file extension: check for REST API syntax

            match = re.search("\.*[?\/]", file.name)

            if(match):
                sub_string = file.name[0:match.start()]
                os.rename(path, datasets_directory+"/"+folder.name+"/"+sub_string)
                path = datasets_directory+"/"+folder.name+"/"+sub_string

            #check for the suffix

            file_suffix = pathlib.Path(path).suffix

            if (file_suffix not in suffixes):

                file_type = magic.from_file(path)

                print("The file: "+file.name+" has not a RDF valid extension: "+file_suffix+"\n")
                f_log.write("The file: "+file.name+" has not a RDF valid extension: "+file_suffix+"\nFile type: "+file_type)

            i+=1

    if i == 0: 
        print("The directory: "+folder.name+" contains only the dataset.json file\n")
        f_log.write("The directory: "+folder.name+" contains only the dataset.json file\n")

f_log.close()
