import json
import re
import os

def cleanDatasetsJSONFiles(datasets_directory):
    index = 0

    for folder in os.scandir(datasets_directory):

        if index % 1000 == 0:
            print("Cleaned: "+str(index)+" datasets")

        dataset_json_file_path = datasets_directory+"/"+folder.name+"/dataset.json"
        
        dataset_json_file = open(dataset_json_file_path,"r", encoding='utf-8')
        dataset_json = json.load(dataset_json_file)
        dataset_json_file.close()
        for key, value in dataset_json.items():
            value = re.sub("\\n|\\r|  +", '', str(value))
            dataset_json[key] = value

        dataset_json_file = open(dataset_json_file_path,"w", encoding='utf-8')
        json_string = json.dumps(dataset_json, indent=4, ensure_ascii=False)
        dataset_json_file.write(json_string)

        dataset_json_file.close()
        del(dataset_json)

        index+=1


def main():
    #datasets_directory = "/home/manuel/Tesi/ACORDAR/Test" 
    datasets_directory = "/media/manuel/500GBHDD/Tesi/Datasets"                                   #path to the folder of the downloaded datasets

    cleanDatasetsJSONFiles(datasets_directory)

if __name__ == "__main__" :
    main()





