'''
This script will adapt the qrels file by removing the dataset that are empty, 
so datasets that has not other file other the dataset.json file with its metadata

All will be based on the list of indexable datasets produced by the checkDatasets script
'''

'''
@param qrels_file_path qrels file
@param datasets_indexable_list_file list of all the indexable datasets
@param indexable_qrels_path new qrels file with only the relevance judgments referred to indexable datasets
'''
def adjustQRELSFile(qrels_path, datasets_indexable_list_path, indexable_qrels_path):
    #open the qrels file
    qrels_file = open(qrels_path, "r")

    #open the datasets_indexable_list file
    datasets_indexable_list_file = open(datasets_indexable_list_path, "r")

    #open the indexable qrels path
    indexable_qrels_file = open(indexable_qrels_path, "r")

    #create a set of indexable datasets ids from the list file
    indexable_datasets_ids = set()
    
    for line in datasets_indexable_list_file:
        id = int(line)
        indexable_datasets_ids.add(id)

    #create the new qrels file
    for line in qrels_file:
        topicID, fixed, datasetID, judgment = [int(s) for s in line.split()]

        #check if the dataset is indexable, if it is add to the new qrels file 
        if datasetID in indexable_datasets_ids:
            indexable_qrels_file.write(line)

    #close the files
    qrels_file.close()
    datasets_indexable_list_file.close()
    indexable_qrels_file.close()

def main():
    qrels_path = "/home/manuel/Tesi/ACORDAR/Data/qrels.txt"
    datasets_indexable_list_path = "/home/manuel/Tesi/ACORDAR/Data/indexable_datasets.txt"
    indexable_qrels_path = "/home/manuel/Tesi/ACORDAR/Data/indexable_qrels.txt"

    adjustQRELSFile(qrels_path, datasets_indexable_list_path, indexable_qrels_path)

if __name__ = "__main__":
    main()