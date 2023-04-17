package dei.unipd.utils;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.SQLOutput;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

/** TODO:
 * manage not working links
 * manage stop and resume download
 *
 */

/**
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
     * @throws RuntimeException if the path provided for the JSON file is not correct or if the path provided for the target directory is not correct
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
     * It parse with a stream json reader the json file with the datasets list
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

        int resumeID = 0;   //setting this id with the id of the last downloaded dataset

        String[] urls = new String[500];
        int dataset_id;
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
                        urls = new String[500];
                        reader.beginArray();
                        int i = 0;
                        while ((jsonToken = reader.peek()) != JsonToken.END_ARRAY) {
                            if (jsonToken == JsonToken.STRING) {
                                urls[i] = reader.nextString();
                                i++;
                            }
                        }
                        reader.endArray();

                    }
                    if(Objects.equals(name, "dataset_id")) {
                        dataset_id = Integer.parseInt(reader.nextString());
                        if(dataset_id>resumeID)
                            download(dataset_id,urls);
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
     * @param datasetsURL url of the dataset from where to download it
     * @return true if the download was done, else false (directory not created)
     */
    private boolean download(int datasetID, String[] datasetsURL){
        //create the directory where to save the dataset RDF or TTL files

        Path directoryDataset = Paths.get(downloadDirectory+"/dataset-"+datasetID+"/");
        try{
            Files.createDirectory(directoryDataset);
        } catch (IOException e) {
            System.out.println("Cannot create the dataset download directory:"+e.getMessage());;
            return false;
        }

        System.out.println("Downloading dataset ID: "+datasetID);
        for (String url: datasetsURL) {
            if(url!=null) {

                try {
                    URL urlObject = new URL(url);
                    HttpURLConnection httpConn = (HttpURLConnection) urlObject.openConnection();
                    String fileName = "";
                    String disposition = httpConn.getHeaderField("Content-Disposition");
                    int contentLength = httpConn.getContentLength();

                    if (disposition != null) {
                        // extracts file name from header field
                        int index = disposition.indexOf("filename=");
                        if (index > 0)
                            fileName = disposition.substring(index + 10, disposition.length() - 1);
                    } else {
                        // extract file name from url
                        fileName = url.substring(url.lastIndexOf('/') + 1);

                    }

                    System.out.println("Content-Disposition = " + disposition);
                    System.out.println("Content-Length = " + contentLength);
                    System.out.println("fileName = " + fileName);

                    // opens input stream from the HTTP connection

                    InputStream in = httpConn.getInputStream();
                    long bytes = Files.copy(in, Paths.get(directoryDataset+"/"+fileName), StandardCopyOption.REPLACE_EXISTING);
                    System.out.printf("Downloaded %d bytes from %s to %s.%n", bytes, url, "targetFilename");

                } catch (IOException e) {
                    System.out.println("Error in the stream: " + e.getMessage());
                }
            }
        }
        return true;
    }

    public static void main(String[] args) throws IOException {
        Downloader d = new Downloader("/home/manuel/Tesi/EDS/EDS/eds/src/main/resources/datasets.json", "/home/manuel/Tesi/ACORDAR/Datasets");
        d.startDownload();

    }


}
