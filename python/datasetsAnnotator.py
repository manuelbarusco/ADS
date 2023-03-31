#This script will add to the json file of every dataset the considered metatags

import os
import json 

datasets_directory_path = "/home/manuel/Tesi/ACORDAR/Test"
json_datasets_list_path = "/home/manuel/Tesi/ACORDAR/Data/datasets.json"

'''
This functio add the metatags to the dataset json file previosly mined
@param dataset json object read from the json list
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

    
def main():

    json_list_file = open(json_datasets_list_path, "r")
    json_list = json.load(json_list_file,strict=False)

    for dataset in json_list['datasets']:

        dataset_id = dataset["dataset_id"]

        dataset_json_path = datasets_directory_path + "/dataset-" + dataset_id + "/dataset.json"

        #open the json file mined in the directory of the dataset
        if os.path.isfile(dataset_json_path): 
            
            print(dataset_json_path)
            dataset_json_file = open(dataset_json_path, "r")
            dataset_json = json.load(dataset_json_file,strict=False)
            dataset_json_file.close()
            addMetaTags(dataset, dataset_json)

            # Serializing json_dict
            dataset_json_file = open(dataset_json_path, "w")
            json_dict_object = json.dumps(dataset_json, indent=4)
            dataset_json_file.write(json_dict_object)

        dataset_json_file.close

if __name__ == "__main__":
    main() 
        
