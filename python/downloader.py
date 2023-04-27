'''
This script will download all the datasets indicated in the datasets.json file provided in input
For every dataset it will:
* create the directory for the dataset
* download all the dataset files in the directory
* crete the json file with all the dataset meta-data included the download info (number of urls dowloaded and urls total number)
If there are errors during the download we will log: dataset-id, URL and error of the problem
'''

import json 
import requests
from tqdm import tqdm
import os
import re
  
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
    f_log=open(error_log_file, "a")

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

            dataset_json = createDatasetJSONObject(dataset) 
            download(dataset["download"], dataset_id, dataset_directory_path, dataset_json, f_log)

            #save the dataset json object in the dataset.json file
            #serializing dataset_json object
            json_dict_object = json.dumps(dataset_json, indent=4)

            #writing the json file
            with open(dataset_directory_path+"/dataset.json", "w") as outfile:
                outfile.write(json_dict_object)

            #free memory by closing the dataset.json file
            del json_dict_object
            del dataset_json
            outfile.close()

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
'''
def createDatasetJSONObject(dataset):

    dataset_json = {}

    #add the meta-tags to the dataset.json 
    addMetaTags(dataset, dataset_json)

    #set the mined false for future mining
    dataset_json["mined"]=False

    return dataset_json 

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
@param dataset_json dict object to be serialized into a json object and file
@param f_log log file for all the errors
'''
def download(urls, dataset_id, dataset_directory_path, dataset_json, f_log):

    url_i = 0 

    downloaded_files = 0 

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

                #escape not valid caracthers in the url
                match = re.search("[:#?&=%*]", filename)

                if match:
                    filename = filename[0:match.start()]

                #check for empty string
                if filename == "":
                    filename = "file_url_"+str(url_i); 

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
            
            #close the  downloaded file
            f.close()
            downloaded_files += 1

        except (requests.ConnectionError,requests.HTTPError,requests.exceptions.RequestException) as e:
            print("Error in dataset: "+dataset_id+"\nURL: "+url+"\nError: "+str(e)+"\n")
            f_log.write("Error in dataset: "+dataset_id+"\nURL: "+url+"\nError: "+str(e)+"\n")
        
        except (KeyError) as e:
            #if the content-length header is not present

            for chunk in r.iter_content(chunk_size=chunkSize): 
                if chunk: 
                    f.write(chunk)
            #close the  downloaded file
            f.close()
            downloaded_files += 1

    dataset_json["download_info"] = {"downloaded": downloaded_files, "total_URLS": len(urls)}
        


def main():
    datasets_json_path = "/home/manuel/Tesi/ACORDAR/Data/datasets.json"                     #path to the datasets list json file
    datasets_folder_path = "/media/manuel/500GBHDD/Tesi/Datasets"                                  #path to the folder that contains the datasets
    error_log_file = "/home/manuel/Tesi/ACORDAR/Log/downloader_error_log.txt"                #path to the error log file
    resume_row = 21655                                                                           #last downloaded dataset 

    readAndDownload(datasets_json_path, datasets_folder_path, error_log_file, resume_row)

if __name__ == "__main__" : 
    main(); 