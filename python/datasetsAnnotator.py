#This script will add to the json file of every dataset the considered metatags

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


def main():

    datasets_directory_path = "/media/manuel/500GBHDD/Tesi/Datasets"
    json_datasets_list_path = "/home/manuel/Tesi/ACORDAR/Data/datasets.json"

    json_list_file = open(json_datasets_list_path, "r")
    json_list = json.load(json_list_file,strict=False)

    print("START ANNOTATION")

    for dataset in json_list['datasets']:

        dataset_id = dataset["dataset_id"]
        dataset_directory = datasets_directory_path + "/dataset-" + dataset_id
        
        #check if the dataset was downloaded
        if os.path.exists(dataset_directory):
            dataset_json_path = datasets_directory_path + "/dataset-" + dataset_id + "/dataset.json"

            #check if the json file for the given dataset exists
            if not os.path.exists(dataset_json_path): 

                print("Annotating dataset: "+dataset_id)
                
                #define the dataset directory path
                dataset_directory_path = datasets_directory_path + "/dataset-" + dataset_id

                createDatasetJSONFile(dataset, dataset_directory_path)


    del(json_list)
    json_list_file.close()

    print("END ANNOTATION")

if __name__ == "__main__":
    main() 
        
