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
@param resume_id the id of the dataset from which start in case of resume
'''
def readAndDownload(datasets_json_path, datasets_folder_path,  error_log_file, resume_id):

    #open the json file
    f=open(datasets_json_path)

    #open the error log file
    f_log=open(error_log_file, "w+")

    #load the json object 
    data = json.load(f,strict=False)

    for dataset in data['datasets']:

        dataset_id = dataset["dataset_id"]

        if(int(dataset_id)>=resume_id):
            
            #define the dataset directory path
            dataset_directory_path = datasets_folder_path + "/dataset-" + dataset_id

            #create a directory for all the datasets files
            if(not os.path.exists(dataset_directory_path)):
                os.makedirs(dataset_directory_path)

            print("Start downloading dataset: "+dataset_id)

            createDatasetJSONFile(dataset, dataset_directory_path) 
            download(dataset["download"], dataset_id, dataset_directory_path,f_log)

            print("End downloading dataset: "+dataset_id)

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

    addMetaTags(dataset, dataset_json)

    # Serializing json_dict
    json_dict_object = json.dumps(dataset_json, indent=4)
    # Writing 
    with open(dataset_directory_path+"/dataset.json", "w") as outfile:
        outfile.write(json_dict_object)

    #free memory 
    del json_dict_object
    del dataset_json
    outfile.close()

'''
This functiom add the metatags to the dataset json file previosly mined
@param dataset json object of the dataset
@param dataset_json json object where to write the metatags
'''
def addMetaTags(dataset, dataset_json):
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

            print("Downloading from url: "+url)

            with open(final_path, 'wb') as f:
                pbar = tqdm( unit="B", total=int( r.headers['Content-Length'] ) )
                for chunk in r.iter_content(chunk_size=chunkSize): 
                    if chunk: 
                        pbar.update (len(chunk))
                        f.write(chunk)

        except (requests.ConnectionError,requests.HTTPError,requests.exceptions.RequestException) as e:
            print("Error in dataset: "+dataset_id+"\nURL: "+url+"\nError: "+e+"\n")
            f_log.write("Error in dataset: "+dataset_id+"\nURL: "+url+"\nError: "+e+"\n")

def main():
    datasets_json_path = "/home/manuel/Tesi/ACORDAR/Data/datasets1.json"                     #path to the datasets list json file
    datasets_folder_path = "/home/manuel/Tesi/ACORDAR/Test1"                                 #path to the folder that contains the datasets
    error_log_file = "/home/manuel/Tesi/ACORDAR/Log/downloader_error_log.txt"                #path to the error log file
    resume_id = 0                                                                            #last downloaded dataset 

    readAndDownload(datasets_json_path, datasets_folder_path, error_log_file, resume_id)

if __name__ == "__main__" : 
    main(); 