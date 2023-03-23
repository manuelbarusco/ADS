package dei.unipd.utils;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Pattern;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

/**
 * TODO: check the error conditions, returns and exceptions
 * @author Manuel Barusco
 * This class will provide all the utilities for downloading the ACORDAR test collection
 * or in general a list of dataset from a json file
 */
public class Downloader {
    private String pathDSFile;              //path to the datasets list file in json format
    private String downloadDirectory;       //path to the destination directory where to download all the datasets

    /**
     * Constructor
     * @param pathJSONFile path to the datasets list file in json format
     * @param targetDirectoryPath path to the destination directory where to download all the dataset
     */
    public Downloader(String pathJSONFile, String targetDirectoryPath){
        //check that paths exist and are correct
        File jsonFile = new File(pathJSONFile);
        if(!jsonFile.isFile())
            throw new RuntimeException("The path provided for the JSON is not correct");

        File targetDirectory = new File(targetDirectoryPath);
        if(!targetDirectory.isDirectory())
            throw new RuntimeException("The path provided for the target directory is not correct");

        pathDSFile = pathJSONFile;
        downloadDirectory = targetDirectoryPath;

    }

    /**
     * Method that starts the download process
     */
    public void startDownload(){
        //create the JSON reader
        JsonReader reader;
        try{
            reader = new JsonReader(new FileReader(pathDSFile));
        } catch (FileNotFoundException e) {
            System.out.println("Error while opening the json file: "+e.getMessage());
            return;
        }

        try {
            JsonToken jsonToken;
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
                    String name = reader.nextName();
                    if(Objects.equals(name, "download")) {
                        String[] urls = new String[500];
                        reader.beginArray();
                        int i = 0;
                        while ((jsonToken = reader.peek()) != JsonToken.END_ARRAY) {
                            if (jsonToken == JsonToken.STRING) {
                                urls[i] = reader.nextString();
                                i++;
                            }
                        }
                        reader.endArray();

                        System.out.println("Array: ");
                        System.out.println(Arrays.toString(urls));

                    }
                } else if (jsonToken == JsonToken.STRING) {
                    reader.nextString();
                }
            }

            reader.close();

        } catch(IOException e){
            System.out.println(e.getMessage());
        }
    }

    /**
     * Method that perform a single dataset download
     * @param datasetID id of the dataset in the JSON file list
     * @param datasetURL url of the dataset from where to download it
     * @return true if the download was done, else false (directory not created)
     */
    private boolean download(int datasetID, String datasetURL){
        //create the directory where to save the dataset RDF or TTL files
        File directoryDataset = new File(downloadDirectory+"/"+datasetID);

        //check if the directory exists before creating a new one
        if(!directoryDataset.isDirectory()){
            if(directoryDataset.mkdir())
                return false;
        }

        System.out.println("Downloading dataset ID: "+datasetID);
        try (InputStream in = URI.create(datasetURL).toURL().openStream()) {
            long bytes = Files.copy(in, Paths.get(directoryDataset.getPath()+""));
            System.out.printf("Downloaded %d bytes from %s to %s.%n", bytes, datasetURL, "targetFilename");
            return true;
        } catch (MalformedURLException e){
            System.out.println("The URL specified is malformed: "+ e.getMessage());
            return false;

        } catch (IOException e){
            System.out.println("Error in the stream: "+e.getMessage());
            return false;
        }
    }

    public static void main(String[] args){
        Downloader d = new Downloader("/home/manuel/Tesi/EDS/EDS/eds/src/main/resources/datasets.json", "./");
        d.startDownload();
    }

}
