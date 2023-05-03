'''
This script will extract the post-download and final statistic of the downloaded and 
pre-processed collection of datasets.

The script will return in an output file:
* For the POST-DOWNLOAD:
    * the total number of datasets
    * the number of full downloaded datasets
    * the number of partially downloaded datasets
    * the number of empty datasets

* the number of not-rdf files:
    * number of zip files not extracted (are too big or cannot be opened)
'''

import os
import json

'''
@param dataset_directory path to the directory where there are all the datasets
@param checker_error_log_path path of the log file of the dataset checker
@param output_file_path file where to write the output statistics
'''
def postDownloadStats(datasets_directory, checker_error_log_path, output_file_path):

    output_file = open(output_file_path, "a")
    f_log_checker = open(checker_error_log_path, "r")

    n_datasets = 0
    n_empty = 0
    n_full = 0

    #scan the datasets folders 

    for folder in os.scandir(datasets_directory):
        
        n_datasets+=1

        if n_datasets % 1000 == 0:
            print("Scanned: "+str(n_datasets))

        dataset_json_path = datasets_directory+"/"+folder.name+"/dataset.json"

        #open the dataset.json file 
        dataset_json_file=open(dataset_json_path, "r")
        
        #load the json object present in the json datasets list
        dataset_json = json.load(dataset_json_file,strict=False)

        if dataset_json["download_info"]["downloaded"] == 0:
            n_empty+=1 
        
        if dataset_json["download_info"]["downloaded"] == dataset_json["download_info"]["total_URLS"]:
            n_full+=1
        
        dataset_json_file.close()
        del(dataset_json)
        
    #read the checker error_log

    n_file = 0
        
    while True:

        line = f_log_checker.readline()

        if not line:
            break

        #split the line
        fields = line.split(": ")

        if fields[0] == "File":
            n_file += 1

    output_file.write("Number of datasets: "+str(n_datasets)+"\n")
    output_file.write("Number of full datasets: "+str(n_full)+"\n")
    output_file.write("Number of partial datasets: "+str(n_datasets-n_full-n_empty)+"\n")
    output_file.write("Number of empty datasets: "+str(n_empty)+"\n")
    output_file.write("Number of files that need to be assigned to an extension: "+str(n_file)+"\n")

    output_file.close()
    f_log_checker.close()
    
'''
@param recover_error_log_path path to the file recover error log path 
@param output_file_path file where to write the output statistics
'''
def finalStats(recover_error_log_path, output_file_path):
    f_log_recover = open(recover_error_log_path, "r")
    output_file = open(output_file_path, "a")

    #read the recover error_log

    n_file = 0
        
    while True:

        line = f_log_recover.readline()

        if not line:
            break

        #split the line
        fields = line.split(": ")

        if fields[0] == "File":
            n_file += 1

    output_file.write("Number of unrecovered files: "+str(n_file))

    f_log_recover.close()
    output_file.close()


def main():
    #datasets_directory = "/home/manuel/Tesi/ACORDAR/Test" 
    datasets_directory = "/media/manuel/500GBHDD/Tesi/Datasets"                                 #path to the folder of the downloaded datasets
    output_file_path = ""                                                                       #path to the output file
    post_download = False 


    if post_download:
        output_file_path = "/home/manuel/Tesi/ACORDAR/Log/Statistiche/post_download_stats.txt"  
        checker_error_log_file = "/home/manuel/Tesi/ACORDAR/Log/checker_error_log.txt"
        postDownloadStats(datasets_directory, checker_error_log_file,  output_file_path) 
    else:
        output_file_path = "/home/manuel/Tesi/ACORDAR/Log/Statistiche/final_stats.txt"   
        recover_error_log_file = "/home/manuel/Tesi/ACORDAR/Log/recover_error_log.txt"
        finalStats(recover_error_log_file, output_file_path) 


if __name__ == "__main__":
    main()