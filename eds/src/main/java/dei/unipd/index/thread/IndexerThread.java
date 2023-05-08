package dei.unipd.index.thread;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import dei.unipd.index.DatasetField;
import dei.unipd.parse.ParsedDataset;
import dei.unipd.parse.StreamRDFParser;
import dei.unipd.utils.Constants;
import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

public class IndexerThread extends Thread{

    private BlockingQueue<File> datasets;
    private IndexWriter indexWriter;
    private IndexSetup.IndexSharedInfo info;

    private long datasetsCount;
    private long filesCount;
    private long bytesCount;
    private long errorsCount;
    private StringBuilder errorMessages;

    /**
     * One megabyte constant
     */
    private static final int MBYTE = 1024 * 1024;


    /**
     * Constructor
     *
     * @param datasetsQueue of dataset that must be indexed
     * @param info shared indexing info
     */
    public IndexerThread(BlockingQueue<File> datasetsQueue, IndexSetup.IndexSharedInfo info) {
        if(datasetsQueue.isEmpty())
            throw new IllegalArgumentException("The queue of datasets is empty");
        if(info == null)
            throw new IllegalArgumentException("No IndexSharedInfo associated");

        datasets = datasetsQueue;
        this.indexWriter = info.getIndexWriter();
        this.info = info;
    }

    /**
     * This method index a single field read in the json file by considering the different types
     * of fields that must be indexed in the document
     *
     * @param reader JsonReader object
     * @param document Lucene document object (in our case it represents a dataset)
     * @param name of the json field read
     * @return true if there are other fields to index in the dataset.json file, else false (we are not indexing info such
     * as mined or download info in the dataset.json file)
     */
    public boolean indexMetaTag(JsonReader reader, Document document, String name){
        //check the name of the field
        try {
            if (Objects.equals(name, "dataset_id")) {
                String id = reader.nextString();
                document.add(new DatasetField(ParsedDataset.FIELDS.ID, id));
            } else if (Objects.equals(name, "title")) {
                String title = reader.nextString();
                document.add(new DatasetField(ParsedDataset.FIELDS.TITLE, title));
            } else if (Objects.equals(name, "description")) {
                String description = reader.nextString();
                document.add(new DatasetField(ParsedDataset.FIELDS.DESCRIPTION, description));
            } else if (Objects.equals(name, "author")) {
                String author = reader.nextString();
                document.add(new DatasetField(ParsedDataset.FIELDS.AUTHOR, author));
            } else if (Objects.equals(name, "tags")) {
                //the tag are splitted and indexed
                String tags = reader.nextString();
                String[] tagsArray = tags.split(";");
                for (String tag : tagsArray) {
                    document.add(new DatasetField(ParsedDataset.FIELDS.TAGS, tag));
                }
            } else {
                return false;
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return true;
    }

    /**
     * This method will prepare the indexing of a single dataset, so it will read all the RDF files inside its
     * directory and also the relative dataset.json file for the metatags and it will update the Lucene document object
     * with their content
     *
     * @param directory object of type File that represent the directory of the dataset
     * @param files array of File objects that have to be indexed
     * @param document Lucene Document object
     * @return the error message that must be logged at the end of the indexing of this dataset
     */
    private void createDatasetDocument(File directory, File[] files, Document document) throws IOException {
        int indexableFiles = files.length - 1; // Not count the dataset.json file inside the directory

        String dataset_json_path = "";

        for (File file : files) {

            if (file.getName().equals("dataset.json")) {

                //recover the path for later update
                dataset_json_path = file.toString();

                //index the meta-data

                //creating the JSON Reader
                JsonReader reader = null;
                try {
                    reader = new JsonReader(new FileReader(file.toString()));
                } catch (FileNotFoundException e) {
                    throw new RuntimeException("Cannot find the dataset.json file for the dataset: "+directory.getName());
                }

                JsonToken jsonToken;

                //loop while we not reach the end of the document
                while ((jsonToken = reader.peek()) != JsonToken.END_DOCUMENT) {
                    if (jsonToken == JsonToken.BEGIN_OBJECT) {
                        reader.beginObject();
                    } else if (jsonToken == JsonToken.END_OBJECT) {
                        reader.endObject();
                    } else if (jsonToken == JsonToken.BEGIN_ARRAY) {
                        reader.beginArray();
                    } else if (jsonToken == JsonToken.END_ARRAY) {
                        reader.endArray();
                    } else if (jsonToken == JsonToken.NAME) {

                        //Add the datasets contents and meta-contents to the lucene document

                        String name = reader.nextName();
                        if (!indexMetaTag(reader, document, name))
                            break;

                    } else if (jsonToken == JsonToken.STRING) {
                        reader.nextString();
                    } else if (jsonToken == JsonToken.BOOLEAN) {
                        reader.nextBoolean();
                    }
                }

                reader.close();

            } else if (isRDFFile(file.getName())){

                //check if the file is greater than 500 megabytes
                if ((file.length() / MBYTE) > 500){
                    errorMessages = new StringBuilder("Dataset: " + directory.getName() + "\nFile: " + file.getName() + "\nError: bigger than 500 MB\n");
                } else {
                    //index the datasets content

                    bytesCount += file.getTotalSpace();

                    filesCount += 1;

                    StreamRDFParser parser;
                    try {
                        parser = new StreamRDFParser(file.getPath().toString());
                        while (parser.hasNext()) {
                            StreamRDFParser.CustomTriple triple = parser.next();

                            //check for possible null values in the triple
                            if(triple.getSubject().getValue() != null)
                                document.add(new DatasetField(triple.getSubject().getKey(), triple.getSubject().getValue()));
                            else
                                document.add(new DatasetField(triple.getSubject().getKey(), ""));

                            if(triple.getPredicate() == null)
                                document.add(new DatasetField(ParsedDataset.FIELDS.PROPERTIES, ""));
                            else
                                document.add(new DatasetField(ParsedDataset.FIELDS.PROPERTIES, triple.getPredicate()));

                            if(triple.getObject().getValue() != null)
                                document.add(new DatasetField(triple.getObject().getKey(), triple.getObject().getValue()));
                            else
                                document.add(new DatasetField(triple.getObject().getKey(), ""));

                        }
                        parser.close();
                    } catch (Exception e){
                        //remove from the exception message all the \n characters that can break the message
                        String errorMessage = e.getMessage().replace("\n", " ");
                        errorMessages.append("Dataset: ").append(directory.getName()).append("\nFile: ").append(file.getName()).append("\nError: ").append(errorMessage).append("\n");
                        errorsCount++;
                        indexableFiles--;
                    } catch (OutOfMemoryError e) {
                        errorMessages.append("Dataset: ").append(directory.getName()).append("\nFile: ").append(file.getName()).append("\nError: JavaOutOfMemory\n");
                        errorsCount++;
                        indexableFiles--;
                    }
                }
            }
        }

        updateJSONFIle(dataset_json_path, indexableFiles, files.length-1);

    }

    /**
     * @param fileName name of the file
     * @return true if the file has a valid RDF extension
     */
    private boolean isRDFFile(String fileName){
        return Constants.suffixes.contains(FilenameUtils.getExtension(fileName));
    }

    /**
     * This method update the dataset.json file after the dataset parsing
     * @param path to the dataset.json file
     * @param indexableFiles number of indexable files for the dataset
     * @param totalFiles total number of files
     */
    private void updateJSONFIle(String path, int indexableFiles, int totalFiles){
        JsonElement json = null;
        try {
            Reader reader = new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8);
            json = JsonParser.parseReader(reader);
            reader.close();
        } catch (Exception e) {
            System.out.println("Error while opening the dataset.json file: "+e);
        }

        //get the JsonObject to udpate
        JsonObject object = json.getAsJsonObject();
        if (indexableFiles == 0)
            object.addProperty("indexable-jena", ParsedDataset.FIELDS.EMPTY);
        else if (indexableFiles > 0 && indexableFiles < totalFiles)
            object.addProperty("indexable-jena", ParsedDataset.FIELDS.PARTIAL);
        else if (indexableFiles == totalFiles)
            object.addProperty("indexable-jena", ParsedDataset.FIELDS.FULL);

        //write the json for updating the dataset.json file
        FileWriter file;
        try {
            file = new FileWriter(path);
            file.write(String.valueOf(object));
            file.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * During the execution this thread will index the next available dataset in the queue
     *
     */
    public void run() {

        //the thread has to run while the shared queue is not empty
        while (true) {
            if(!datasets.isEmpty()){
                //reset the counters
                datasetsCount=0;
                filesCount=0;
                bytesCount=0;
                errorsCount = 0;
                errorMessages = new StringBuilder();

                File dataset = datasets.remove();

                Document document = new Document();     //Lucene Document
                File[] files = dataset.listFiles();

                //prepare the Lucene document for the dataset
                try {
                    createDatasetDocument(dataset, files, document);
                    //we can index the dataset and update the index info
                    indexWriter.addDocument(document); //index the document
                    indexWriter.commit();
                    indexWriter.flush();
                    info.add(filesCount, datasetsCount, bytesCount,errorsCount);
                    info.logMessage(errorMessages.toString());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                
            } else{
                //there are no other datasets to index

                break;
            }

        }
    }



}
