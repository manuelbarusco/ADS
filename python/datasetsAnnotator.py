'''
This script will adjust and align the annotation of all the downloaded datasets
This script was developed for adapting the datasets content to the various version of behaviour of the
downloader

* If the dataset has not the dataset.json file with all the meta tags it will create it
* The dataset json must contain the download_info file
'''

import os
import json 

'''
This function will create the dataset.json file with the dataset meta-tags
@param dataset json object of the dataset
@param dataset_directory_path directory of the dataset
'''
def createDatasetJSONFile(dataset, dataset_directory_path):

    dataset_json = {}

    #add the meta-tags to the dataset.json 
    addMetaTags(dataset, dataset_json)

    #add the download info
    nfiles = len(os.listdir(dataset_directory_path))

    dataset_json["download_info"] = {"downloaded": nfiles , "total_URLS": len(dataset["download"])}

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
This function adjust the download info of the downloaded dataset that have a
dataset.json file already saved. We check:
* name of the download_info fields and 
* correctness of the info in that fields 

@param dataset json object extracted from the datasets json list, it represent a dataset object in the list 
@param dataset_json dictionary object of the dataset.json file to be created
@param dataset_directory_path path to the dataset directory
'''
def checkDownloadInfo(dataset, dataset_directory_path, dataset_json_path):
    
    #open the dataset.json file
    dataset_json_file = open(dataset_json_path, "r")
    dataset_json = json.load(dataset_json_file,strict=False)
    
    #close the dataset.json file because we are going to modify it
    dataset_json_file.close()
    
    #check if the dataset.json file contains the download info
    if "download_info" in dataset_json:
        ndownloaded = 0

        #check for the name of the downloaded field 
        if "Downloaded" in dataset_json["download_info"]:
            ndownloaded = dataset_json["download_info"]["Downloaded"]
            del(dataset_json["download_info"]["Downloaded"])
            del(dataset_json["download_info"]["Total_URLS"])
        elif "downloaded" in dataset_json["download_info"]:
            ndownloaded = dataset_json["download_info"]["downloaded"]

        #check the correctness of the info of the downloaded dataset and adjust it
        if ndownloaded != (len(os.listdir(dataset_directory_path))-1) :
            ndownloaded = len(os.listdir(dataset_directory_path)) - 1  #-1 because we are not considering the dataset.json file

        #correct the name of the fields
        dataset_json["download_info"]["downloaded"] = ndownloaded
        dataset_json["download_info"]["total_URLS"] = len(dataset["download"])
    
    else:
        nfiles = len(os.listdir(dataset_directory_path))

        dataset_json["download_info"] = {"downloaded": nfiles - 1 , "total_URLS": len(dataset["download"])}

    #write the new dataset.json file
    #serializing dataset_json object
    json_dict_object = json.dumps(dataset_json, indent=4)

    #writing 
    with open(dataset_json_path, "w") as outfile:
        outfile.write(json_dict_object)

    outfile.close()
    del(dataset_json)
    del(json_dict_object)


def main():

    datasets_directory_path = "/media/manuel/500GBHDD/Tesi/Datasets"
    json_datasets_list_path = "/home/manuel/Tesi/ACORDAR/Data/datasets.json"

    json_list_file = open(json_datasets_list_path, "r")
    json_list = json.load(json_list_file,strict=False)

    print("START ANNOTATION")

    i =  0
    dataset_number = 13565
    single_dataset = True

    for dataset in json_list['datasets']:

        dataset_id = dataset["dataset_id"]

        if single_dataset:

            if int(dataset_id) == dataset_number:

                dataset_directory_path = datasets_directory_path + "/dataset-" + dataset_id

                #define the dataset directory path and dataset.json file path
                dataset_json_path = datasets_directory_path + "/dataset-" + dataset_id + "/dataset.json"

                #check if the dataset was downloaded, so there is its directory 
                if os.path.exists(dataset_directory_path):

                    #check if the json file for the given dataset exists
                    if not os.path.exists(dataset_json_path): 

                        print("Annotating dataset: "+dataset_id +" creating the json file")
                        createDatasetJSONFile(dataset, dataset_directory_path)

                    else :

                        corrupted = False

                        #check that the file is corrected 
                        try:
                            dataset_json_file = open(dataset_json_path, "r")
                            dataset_json = json.load(dataset_json_file,strict=False)
                        except json.JSONDecodeError as e:
                            corrupted = True
                            os.remove(dataset_json_path)
                        finally:
                            dataset_json_file.close()
                        
                        if corrupted:
                            print("Re-annotating dataset: "+dataset_id +" creating the json file")
                            createDatasetJSONFile(dataset, dataset_directory_path)
                        else:
                            checkDownloadInfo(dataset, dataset_directory_path, dataset_json_path)


        else :

            dataset_directory_path = datasets_directory_path + "/dataset-" + dataset_id

            #define the dataset directory path and dataset.json file path
            dataset_json_path = datasets_directory_path + "/dataset-" + dataset_id + "/dataset.json"

            #check if the dataset was downloaded, so there is its directory 
            if os.path.exists(dataset_directory_path):

                #check if the json file for the given dataset exists
                if not os.path.exists(dataset_json_path): 

                    print("Annotating dataset: "+dataset_id +" creating the json file")
                    createDatasetJSONFile(dataset, dataset_directory_path)

                else :

                    corrupted = False

                    #check that the file is corrected 
                    try:
                        dataset_json_file = open(dataset_json_path, "r")
                        dataset_json = json.load(dataset_json_file,strict=False)
                    except json.JSONDecodeError as e:
                        corrupted = True
                        os.remove(dataset_json_path)
                    finally:
                        dataset_json_file.close()
                    
                    if corrupted:
                        print("Re-annotating dataset: "+dataset_id +" creating the json file")
                        createDatasetJSONFile(dataset, dataset_directory_path)
                    else:
                        checkDownloadInfo(dataset, dataset_directory_path, dataset_json_path)

            i+=1

            if i%1000 == 0:
                print("Annotated: "+str(i)+" datasets\n")

    

    del(json_list)
    json_list_file.close()

    print("END ANNOTATION")

if __name__ == "__main__":
    main() 
        
