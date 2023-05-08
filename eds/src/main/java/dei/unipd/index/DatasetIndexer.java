/*
 *  Copyright 2021-2022 University of Padua, Italy
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dei.unipd.index;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import dei.unipd.parse.ParsedDataset;
import dei.unipd.parse.StreamRDFParser;
import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import dei.unipd.utils.Constants;
import org.apache.lucene.search.*;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import dei.unipd.analyze.AnalyzerUtil;

import javax.print.Doc;

/**
 * Indexer object for indexing the ACORDAR datasets
 *
 * @author Manuel Barusco (manuel.barusco@studenti.unipd.it)
 * @version 1.00
 * @since 1.00
 */
public class DatasetIndexer {

    /**
     * One megabyte constant
     */
    private static final int MBYTE = 1024 * 1024;

    /**
     * Index Writer Configuration
     */
    private final IndexWriterConfig iwc;

    /**
     * The Index Writer Lucene object
     */
    private IndexWriter writer = null;

    /**
     * The Path object to the index directory
     */
    private final Path indexDir;

    /**
     * The Path object of the directory where datasets are stored.
     */
    private final Path datasetsDir;

    /**
     * The charset used for encoding documents.
     */
    private final Charset cs;

    /**
     * The total number of documents expected to be indexed.
     */
    private final long expectedDatasets;

    /**
     * The start instant of an indexing phase
     */
    private final long start;

    /**
     * The total number of indexed files (we count all the files inside the
     * datasets folder that are indexed)
     */
    private long filesCount;

    /**
     * The total number of indexed documents (datasets)
     */
    private long datasetsCount;

    /**
     * The total number of indexed bytes
     */
    private long bytesCount;

    /**
     * Creates a new indexer
     *
     * @param analyzer                  the {@code Analyzer} to be used in the indexing phase
     * @param ramBufferSizeMB           the size in megabytes of the RAM buffer for indexing documents.
     * @param indexPath                 the directory where to store the index.
     * @param datasetsDirectoryPath     the directory from which datasets have to be read.
     * @param charsetName               the name of the charset used for encoding documents.
     * @param expectedDocs              the total number of datasets expected to be indexed
     * @throws NullPointerException     if any of the parameters is {@code null}.
     * @throws IllegalArgumentException if any of the parameters assumes invalid values.
     */
    public DatasetIndexer(final Analyzer analyzer, final int ramBufferSizeMB,
                          final String indexPath, final String datasetsDirectoryPath,
                          final String charsetName, final long expectedDocs) {

        if (analyzer == null) {
            throw new NullPointerException("Analyzer cannot be null.");
        }

        /*
        if (similarity == null) {
            throw new NullPointerException("Similarity cannot be null.");
        }
        */

        if (ramBufferSizeMB <= 0) {
            throw new IllegalArgumentException("RAM buffer size cannot be less than or equal to zero.");
        }

        //setting up the Lucene IndexWriter object
        iwc = new IndexWriterConfig(analyzer);
        //iwc.setSimilarity(similarity);
        iwc.setRAMBufferSizeMB(ramBufferSizeMB);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND); //with this mode the indexer will create an index if it's not present or it will append the new values
        iwc.setCommitOnClose(true);
        iwc.setUseCompoundFile(true);

        if (indexPath == null) {
            throw new NullPointerException("Index path cannot be null.");
        }

        if (indexPath.isEmpty()) {
            throw new IllegalArgumentException("Index path cannot be empty.");
        }

        indexDir = Paths.get(indexPath);

        // if the directory for the index files does not already exist, create it
        if (Files.notExists(indexDir)) {
            try {
                Files.createDirectory(indexDir);
            } catch (Exception e) {
                throw new IllegalArgumentException(String.format("Unable to create directory %s: %s.", indexDir.toAbsolutePath(), e.getMessage()), e);
            }
        }

        if (!Files.isWritable(indexDir)) {
            throw new IllegalArgumentException(String.format("Index directory %s cannot be written.", indexDir.toAbsolutePath()));
        }

        if (!Files.isDirectory(indexDir)) {
            throw new IllegalArgumentException(String.format("%s expected to be a directory where to write the index.", indexDir.toAbsolutePath()));
        }

        if (datasetsDirectoryPath == null) {
            throw new NullPointerException("Datasets path cannot be null.");
        }

        if (datasetsDirectoryPath.isEmpty()) {
            throw new IllegalArgumentException("Datasets path cannot be empty.");
        }

        final Path datasetsDir = Paths.get(datasetsDirectoryPath);
        if (!Files.isReadable(datasetsDir)) {
            throw new IllegalArgumentException(
                    String.format("Datasets directory %s cannot be read.", datasetsDir.toAbsolutePath().toString()));
        }

        if (!Files.isDirectory(datasetsDir)) {
            throw new IllegalArgumentException(
                    String.format("%s expected to be a directory of Datasets.", datasetsDir.toAbsolutePath().toString()));
        }

        this.datasetsDir = datasetsDir;

        if (charsetName == null) {
            throw new NullPointerException("Charset name cannot be null.");
        }

        if (charsetName.isEmpty()) {
            throw new IllegalArgumentException("Charset name cannot be empty.");
        }

        try {
            cs = Charset.forName(charsetName);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format("Unable to create the charset %s: %s.", charsetName, e.getMessage()), e);
        }

        if (expectedDocs <= 0) {
            throw new IllegalArgumentException(
                    "The expected number of documents to be indexed cannot be less than or equal to zero.");
        }
        this.expectedDatasets = expectedDocs;

        //set to zero the counters
        this.datasetsCount = 0;

        this.bytesCount = 0;

        this.filesCount = 0;

        this.start = System.currentTimeMillis();

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
     * @param logFile log file where to report the indexing errors
     * @param errors current counter of errors occurred during the indexing phase
     * @return the counter of errors provided input updated with the error encountered in this dataset
     */
    private int createDatasetDocument(File directory, File[] files, Document document, FileWriter logFile, int errors) throws IOException {
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
                    logFile.write("Dataset: "+directory.getName()+"\nFile: "+file.getName()+"\nError: bigger than 500 MB\n");
                } else {
                    //index the datasets content

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
                        logFile.write("Dataset: "+directory.getName()+"\nFile: "+file.getName()+"\nError: "+ errorMessage+"\n");
                        logFile.flush();
                        errors++;
                        indexableFiles--;
                    } catch (OutOfMemoryError e) {
                        logFile.write("Dataset: "+directory.getName()+"\nFile: "+file.getName()+"\nError: JavaOutOfMemory\n");
                        logFile.flush();
                        errors++;
                        indexableFiles--;
                    }

                    bytesCount += file.getTotalSpace();

                    filesCount += 1;

                }
            } else if (file.getName().contains("-scriptmined")){
                //creating the JSON Reader
                JsonReader reader = null;
                try {
                    reader = new JsonReader(new FileReader(file.getPath()));
                } catch (FileNotFoundException e) {
                    throw new RuntimeException("Cannot find the json file at path: "+file.getPath());
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

                        //Add the datasets contents

                        String name = reader.nextName();
                        if (Objects.equals(name, "classes")) {
                            reader.beginArray();
                            while ((jsonToken = reader.peek()) != JsonToken.END_ARRAY) {
                                if (jsonToken == JsonToken.STRING) {
                                    document.add(new DatasetField(ParsedDataset.FIELDS.CLASSES, reader.nextString()));
                                }
                            }
                            reader.endArray();
                        }
                        if (Objects.equals(name, "entities")) {
                            reader.beginArray();
                            while ((jsonToken = reader.peek()) != JsonToken.END_ARRAY) {
                                if (jsonToken == JsonToken.STRING) {
                                    document.add(new DatasetField(ParsedDataset.FIELDS.ENTITIES, reader.nextString()));
                                }
                            }
                            reader.endArray();
                        }
                        if (Objects.equals(name, "literals")) {
                            reader.beginArray();
                            while ((jsonToken = reader.peek()) != JsonToken.END_ARRAY) {
                                if (jsonToken == JsonToken.STRING) {
                                    document.add(new DatasetField(ParsedDataset.FIELDS.LITERALS, reader.nextString()));
                                }
                            }
                            reader.endArray();
                        }
                        if (Objects.equals(name, "properties")) {
                            reader.beginArray();
                            while ((jsonToken = reader.peek()) != JsonToken.END_ARRAY) {
                                if (jsonToken == JsonToken.STRING) {
                                    document.add(new DatasetField(ParsedDataset.FIELDS.PROPERTIES, reader.nextString()));
                                }
                            }
                            reader.endArray();
                        }
                    } else if (jsonToken == JsonToken.STRING) {
                        reader.nextString();
                    } else if (jsonToken == JsonToken.BOOLEAN) {
                        reader.nextBoolean();
                    }
                }
            }
        }

        updateJSONFIle(dataset_json_path, indexableFiles, files.length-1);

        return errors;

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
     * Index the datasets. This method will go through all the datasets directories.
     *
     * @throws IOException if something goes wrong while indexing.
     */
    public void index() throws IOException {

        //intialize the IndexWriter Object
        try {
            writer = new IndexWriter(FSDirectory.open(indexDir), iwc);
        } catch (IOException e) {
            throw new IllegalArgumentException(String.format("Unable to create the index writer in directory %s: %s.", indexDir.toAbsolutePath().toString(), e.getMessage()), e);
        }

        System.out.printf("%n#### Start indexing ####%n");

        //open the error log file
        FileWriter logFile = new FileWriter(Constants.indexerLogFilePath, true);

        File[] datasetsDirectories = new File(datasetsDir.toString()).listFiles();

        int errors = 0;

        //list of datasets that have to be skipped for some reasons
        HashSet<String> pass = new HashSet<>();
        pass.add("dataset-11580");

        for (File directory: datasetsDirectories) {

            //check the resume index and skip the datasets with problem files
            if(!pass.contains(directory.getName())) {
                Document document = new Document();     //Lucene Document
                File[] files = directory.listFiles();

                //prepare the Lucene document for the dataset
                errors = createDatasetDocument(directory, files, document, logFile, errors);

                //we can index the dataset
                writer.addDocument(document); //index the document

                datasetsCount++;

                //commit index after every 50 dataset for efficiency reasons, the parameter can be tuned
                if (datasetsCount % 50 == 0) {
                    writer.commit();
                }

                // print progress every 10000 indexed documents, only for debug purpose
                if (datasetsCount % 1000 == 0) {
                    System.out.printf("%d document(s) %d error(s) in parsing (%d files, %d Mbytes) indexed in %d seconds.%n",
                            datasetsCount, errors, filesCount, bytesCount / MBYTE,
                            (System.currentTimeMillis() - start) / 1000);
                }
            }
        }

        //indexer commit and resource release
        writer.close();

        //close the log file
        logFile.close();

        if (datasetsCount != expectedDatasets) {
            System.out.printf("Expected to index %d documents; %d indexed instead.%n", expectedDatasets, datasetsCount);
        }

        System.out.printf("%d document(s) (%d files, %d Mbytes) indexed in %d seconds.%n", datasetsCount, filesCount,
                bytesCount / MBYTE, (System.currentTimeMillis() - start) / 1000);

        System.out.printf("#### Indexing complete ####%n");
    }

    /**
     * Method for index update
     *
     * @param documentID id of the document (in the collection, not Lucene internal index)
     * @param jsonMinedFilePath path to the file json with the mined info from the problematic file
     */
    public void updateDocument(String documentID, String jsonMinedFilePath) throws IOException {

        //get the given document

        Query searchQuery = new TermQuery(new Term(ParsedDataset.FIELDS.ID , documentID));
        IndexReader indexReader;
        try {
            indexReader = DirectoryReader.open(FSDirectory.open(indexDir));
        } catch (IOException e) {
            throw new IllegalArgumentException(String.format("Unable to create the index reader for directory %s: %s.",
                    indexDir.toAbsolutePath(), e.getMessage()), e);
        }

        IndexSearcher searcher = new IndexSearcher(indexReader);

        TopDocs result = searcher.search(searchQuery ,1);

        ScoreDoc[] docs = result.scoreDocs;

        Document doc = searcher.doc(docs[0].doc);

        indexReader.close();

        //open the json file

        //creating the JSON Reader
        JsonReader reader = null;
        try {
            reader = new JsonReader(new FileReader(jsonMinedFilePath));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Cannot find the json file at path: "+jsonMinedFilePath);
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
                if (Objects.equals(name, "classes")) {
                    reader.beginArray();
                    while ((jsonToken = reader.peek()) != JsonToken.END_ARRAY) {
                        if (jsonToken == JsonToken.STRING) {
                            writer.updateDocValues(new Term(ParsedDataset.FIELDS.ID, documentID), new DatasetField(ParsedDataset.FIELDS.CLASSES, reader.nextString()));
                        }
                    }
                    reader.endArray();
                }
                if (Objects.equals(name, "entities")) {
                    reader.beginArray();
                    while ((jsonToken = reader.peek()) != JsonToken.END_ARRAY) {
                        if (jsonToken == JsonToken.STRING) {
                            writer.updateDocValues(new Term(ParsedDataset.FIELDS.ID, documentID), new DatasetField(ParsedDataset.FIELDS.ENTITIES, reader.nextString()));
                        }
                    }
                    reader.endArray();
                }
                if (Objects.equals(name, "literals")) {
                    reader.beginArray();
                    while ((jsonToken = reader.peek()) != JsonToken.END_ARRAY) {
                        if (jsonToken == JsonToken.STRING) {
                            writer.updateDocValues(new Term(ParsedDataset.FIELDS.ID, documentID), new DatasetField(ParsedDataset.FIELDS.LITERALS, reader.nextString()));
                        }
                    }
                    reader.endArray();
                }
                if (Objects.equals(name, "properties")) {
                    reader.beginArray();
                    while ((jsonToken = reader.peek()) != JsonToken.END_ARRAY) {
                        if (jsonToken == JsonToken.STRING) {
                            writer.updateDocValues(new Term(ParsedDataset.FIELDS.ID, documentID), new DatasetField(ParsedDataset.FIELDS.PROPERTIES, reader.nextString()));
                        }
                    }
                    reader.endArray();
                }
            } else if (jsonToken == JsonToken.STRING) {
                reader.nextString();
            } else if (jsonToken == JsonToken.BOOLEAN) {
                reader.nextBoolean();
            }
        }

        writer.addDocument(doc);

    }

    /**
     * This method update the index after the RDFLib parsing and mining
     */
    public void updateIndex() throws IOException {

        //intialize the IndexWriter Object
        try {
            writer = new IndexWriter(FSDirectory.open(indexDir), iwc);
        } catch (IOException e) {
            throw new IllegalArgumentException(String.format("Unable to create the index writer in directory %s: %s.", indexDir.toAbsolutePath().toString(), e.getMessage()), e);
        }

        System.out.printf("%n#### Start indexing ####%n");

        File[] datasetsDirectories = new File(datasetsDir.toString()).listFiles();

        //list of datasets that have to be skipped for some reasons
        HashSet<String> pass = new HashSet<>();
        pass.add("dataset-11580");

        for (File directory: datasetsDirectories) {

            //check the resume index and skip the datasets with problem files
            if(!pass.contains(directory.getName())) {

                File[] files = directory.listFiles();

                for (File file: files){
                    if(file.getName().contains("-scriptmined")){
                        String id=directory.getName().split("-")[1];
                        updateDocument(id,file.getPath());
                        datasetsCount ++;
                        System.out.println("Indexed dataset: "+id);
                    }
                }
            }
        }

        //indexer commit and resource release
        writer.close();

        System.out.printf("%d document(s) (%d files, %d Mbytes) indexed in %d seconds.%n", datasetsCount, filesCount,
                bytesCount / MBYTE, (System.currentTimeMillis() - start) / 1000);

        System.out.printf("#### Indexing complete ####%n");

    }

    /**
     * ONLY FOR DEBUGGING PURPOSE
     *
     * @param args command line arguments.
     * @throws Exception if something goes wrong while indexing.
     */
    public static void main(String[] args) throws Exception {

        final int ramBuffer = 512;
        final String indexPath = Constants.indexPath;
        final String datasetDirectoryPath = Constants.datasetsDirectoryPath;

        final int expectedDatasets = 4;
        final String charsetName = "ISO-8859-1";

        CharArraySet cas = AnalyzerUtil.loadStopList("/home/manuel/Tesi/EDS/EDS/eds/src/main/resources/stoplists/nltk-stopwords.txt");
        final Analyzer a = new StandardAnalyzer(cas);

        //final Similarity sim = new LMDirichletSimilarity(1800);

        DatasetIndexer i = new DatasetIndexer(a, ramBuffer, indexPath, datasetDirectoryPath,
                charsetName, expectedDatasets);

        i.index();

    }

}
