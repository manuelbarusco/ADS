#This script will download all the datasets indicated in the datasets.json file provided in input

import json 
import requests
from tqdm import tqdm
import os
  
'''
This function will read the json file and download all the datasets
@param datasets_json_path path to the json file
@param datasets_folder_path path to the folder where to download the datasets
@param error_log_file file with all the possible error that can be trown
@param resume_row the id of the dataset from which start in case of resume
'''
def readAndDownload(datasets_json_path, datasets_folder_path,  error_log_file, resume_row):

    #open the json file with the datasets list
    f=open(datasets_json_path)

    #open the error log file
    f_log=open(error_log_file, "w+")

    #load the json object present in the json datasets list
    data = json.load(f,strict=False)

    row = 1

    for dataset in data['datasets']:

        #resume mechanism

        if(row >= resume_row):

            dataset_id = dataset["dataset_id"]
            
            #define the dataset directory path
            dataset_directory_path = datasets_folder_path + "/dataset-" + dataset_id

            #create a directory for all the datasets files if there isn't
            if(not os.path.exists(dataset_directory_path)):
                os.makedirs(dataset_directory_path)

            print("Processing row order dataset: "+ str(row))
            print("\nStart downloading dataset: "+dataset_id)

            createDatasetJSONFile(dataset, dataset_directory_path) 
            download(dataset["download"], dataset_id, dataset_directory_path,f_log)

            print("\nEnd downloading dataset: "+dataset_id)

        #free memory
        del dataset

        row += 1

    # Closing files
    f.close()
    f_log.close()

'''
This function will create the dataset.json file with the dataset meta-tags
@param dataset json object of the dataset
@param dataset_directory_path directory of the dataset
'''
def createDatasetJSONFile(dataset, dataset_directory_path):

    dataset_json = {}

    #add the meta-tags to the dataset.json 
    addMetaTags(dataset, dataset_json)

    #set the mined false for future mining
    dataset_json["mined"]=False

    #serializing dataset_json object
    json_dict_object = json.dumps(dataset_json, indent=4)

    #writing 
    with open(dataset_directory_path+"/dataset.json", "w") as outfile:
        outfile.write(json_dict_object)

    #free memory 
    del json_dict_object
    del dataset_json
    outfile.close()

'''
This function add the metatags to the dataset.json file previosly downloaded
@param dataset json object of the dataset
@param dataset_json json object where to write the metatags
'''
def addMetaTags(dataset, dataset_json):
    #check the presence of the metatag before adding

    if "dataset_id" in dataset:
        dataset_json["dataset_id"] = dataset["dataset_id"]

    if "title" in dataset:
        dataset_json["title"] = dataset["title"]

    if "description" in dataset:
        dataset_json["description"] = dataset["description"]

    if "author" in dataset:
        dataset_json["author"] = dataset["author"]

    if "tags" in dataset:
        dataset_json["tags"] = dataset["tags"]

'''
This function will download one dataset file
@param url array with all the urls to the datasets
@param dataset_id id of the dataset
@param dataset_directory_path path where the dataset is downloaded 
@param f_log log file for all the errors
'''
def download(urls, dataset_id, dataset_directory_path, f_log):

    for url in urls: 

        try:
            #do the request
            r = requests.get(url, stream=True)

            #define the filename 
            filename=""

            #extract the file name from the content-disposition header
            if(hasattr(r.headers, "content-disposition")):
                filename = r.headers["content-disposition"].split("=", -1)[-1]
            else:
                #extract file name from the URL
                if url.find('/'):
                    filename = url.rsplit('/', 1)[1]

            #download the dataset
            chunkSize = 1024

            final_path = dataset_directory_path+"/"+filename

            print("\nDownloading from url: "+url)

            f = open(final_path, 'wb') 
            pbar = tqdm( unit="B", total=int( r.headers['Content-Length'] ) )
            for chunk in r.iter_content(chunk_size=chunkSize): 
                if chunk: 
                    pbar.update (len(chunk))
                    f.write(chunk)

        except (requests.ConnectionError,requests.HTTPError,requests.exceptions.RequestException) as e:
            print("Error in dataset: "+dataset_id+"\nURL: "+url+"\nError: "+str(e)+"\n")
            f_log.write("Error in dataset: "+dataset_id+"\nURL: "+url+"\nError: "+str(e)+"\n")
        except (KeyError) as e:
            for chunk in r.iter_content(chunk_size=chunkSize): 
                    if chunk: 
                        f.write(chunk)
        

def main():
    datasets_json_path = "/home/manuel/Tesi/ACORDAR/Data/datasets.json"                     #path to the datasets list json file
    datasets_folder_path = "/home/manuel/Tesi/ACORDAR/Datasets"                                  #path to the folder that contains the datasets
    error_log_file = "/home/manuel/Tesi/ACORDAR/Log/downloader_error_log.txt"                #path to the error log file
    resume_row = 5538                                                                           #last downloaded dataset 

    readAndDownload(datasets_json_path, datasets_folder_path, error_log_file, resume_row)

if __name__ == "__main__" : 
    main(); 