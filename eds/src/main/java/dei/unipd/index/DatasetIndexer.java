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

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import dei.unipd.parse.RDFParser;
import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import dei.unipd.utils.Constants;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;

import dei.unipd.analyze.AnalyzerUtil;

/**
 * Indexer object for indexing the ACORDAR datasets previously mined
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
     * The index writer Lucene object
     */
    private final IndexWriter writer;

    /**
     * The JSON Reader object for reading the datasets json files
     */
    private JsonReader reader;

    /**
     * The directory (and eventually sub-directories) where documents are stored.
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
     * The start instant of the indexing.
     */
    private final long start;

    /**
     * The total number of indexed files (datasets)
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
        final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        //iwc.setSimilarity(similarity);
        iwc.setRAMBufferSizeMB(ramBufferSizeMB);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setCommitOnClose(true);
        iwc.setUseCompoundFile(true);

        if (indexPath == null) {
            throw new NullPointerException("Index path cannot be null.");
        }

        if (indexPath.isEmpty()) {
            throw new IllegalArgumentException("Index path cannot be empty.");
        }

        final Path indexDir = Paths.get(indexPath);

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

        try {
            writer = new IndexWriter(FSDirectory.open(indexDir), iwc);
        } catch (IOException e) {
            throw new IllegalArgumentException(String.format("Unable to create the index writer in directory %s: %s.",
                    indexDir.toAbsolutePath().toString(), e.getMessage()), e);
        }

        this.start = System.currentTimeMillis();

    }

    /**
     * This method index a single field read in the json file by considering the different types
     * of fields that must be indexed in the document
     *
     * @param reader json reader object
     * @param document lucene document object (in our case it represents a dataset)
     * @param name of the json field read
     */
    public void indexSingleFieldJSON(JsonReader reader, Document document, String name){
        //check the name of the field
        try {
            if (Objects.equals(name, "dataset_id")) {
                String id = reader.nextString();
                document.add(new DatasetField("dataset_id", id));
            }
            if (Objects.equals(name, "title")) {
                String title = reader.nextString();
                document.add(new DatasetField("title", title));
            }
            if (Objects.equals(name, "description")) {
                String description = reader.nextString();
                document.add(new DatasetField("description", description));
            }
            if (Objects.equals(name, "author")) {
                String author = reader.nextString();
                document.add(new DatasetField("author", author));
            }

            //the tag are splitted and indexed
            if (Objects.equals(name, "tags")) {
                String tags = reader.nextString();
                String[] tagsArray = tags.split(";");
                for (String tag : tagsArray) {
                    document.add(new DatasetField("tags", tag));
                }
            }



            //manage the classes, entities, literals and properties
            if (Objects.equals(name, "classes")) {
                reader.beginArray();
                JsonToken jsonToken;
                while ((jsonToken = reader.peek()) != JsonToken.END_ARRAY) {
                    if (jsonToken == JsonToken.STRING) {
                        document.add(new DatasetField("classes", reader.nextString()));
                    }
                }
                reader.endArray();
            }

            if (Objects.equals(name, "entities")) {
                reader.beginArray();
                JsonToken jsonToken;
                while ((jsonToken = reader.peek()) != JsonToken.END_ARRAY) {
                    if (jsonToken == JsonToken.STRING) {
                        document.add(new DatasetField("entities", reader.nextString()));
                    }
                }
                reader.endArray();
            }

            if (Objects.equals(name, "literals")) {
                reader.beginArray();
                JsonToken jsonToken;
                while ((jsonToken = reader.peek()) != JsonToken.END_ARRAY) {
                    if (jsonToken == JsonToken.STRING) {
                        document.add(new DatasetField("literals", reader.nextString()));
                    }
                }
                reader.endArray();
            }

            if (Objects.equals(name, "properties")) {
                reader.beginArray();
                JsonToken jsonToken;
                while ((jsonToken = reader.peek()) != JsonToken.END_ARRAY) {
                    if (jsonToken == JsonToken.STRING) {
                        document.add(new DatasetField("properties", reader.nextString()));
                    }
                }
                reader.endArray();
            }

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Indexes the datasets.
     *
     * @throws IOException if something goes wrong while indexing.
     */
    public void indexJson() throws IOException {

        System.out.printf("%n#### Start indexing ####%n");

        Files.walkFileTree(datasetsDir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

                //open only the dataset.json file for all the datasets
                if (file.getFileName().toString().equals("dataset.json")) {

                    //creating the JSON Parser
                    JsonReader reader = new JsonReader(new FileReader(file.toString()));

                    bytesCount += Files.size(file);

                    filesCount += 1;

                    Document doc = new Document(); //Lucene Document

                    try {
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
                                indexSingleFieldJSON(reader,doc, name);

                            } else if (jsonToken == JsonToken.STRING) {
                                reader.nextString();
                            } else if (jsonToken == JsonToken.BOOLEAN) {
                                reader.nextBoolean();
                        }
                        }

                        reader.close();

                    } catch(IOException e){
                        System.out.println(e.getMessage());
                    }

                    writer.addDocument(doc); //index the document

                    datasetsCount++;

                    //commit index after every 50 dataset for efficiency reasons
                    //TODO: tune the parameter
                    if (datasetsCount % 50 == 0)
                        writer.commit();

                    // print progress every 10000 indexed documents, only for debug purpose
                    if (datasetsCount % 10000 == 0) {
                        System.out.printf("%d document(s) (%d files, %d Mbytes) indexed in %d seconds.%n",
                                datasetsCount, filesCount, bytesCount / MBYTE,
                                (System.currentTimeMillis() - start) / 1000);
                    }

                }

                return FileVisitResult.CONTINUE;
            }
        });

        //indexer commit and resource release
        writer.close();

        if (datasetsCount != expectedDatasets) {
            System.out.printf("Expected to index %d documents; %d indexed instead.%n", expectedDatasets, datasetsCount);
        }

        System.out.printf("%d document(s) (%d files, %d Mbytes) indexed in %d seconds.%n", datasetsCount, filesCount,
                bytesCount / MBYTE, (System.currentTimeMillis() - start) / 1000);

        System.out.printf("#### Indexing complete ####%n");
    }

    /**
     * This method index a single field read in the json file by considering the different types
     * of fields that must be indexed in the document
     *
     * @param reader json reader object
     * @param document lucene document object (in our case it represents a dataset)
     * @param name of the json field read
     * @return true if there are other fields to index in the dataset.json file, else false (we are not indexing info such as mined or download info)
     */
    public boolean indexMetaTags(JsonReader reader, Document document, String name){
        //check the name of the field
        try {
            if (Objects.equals(name, "dataset_id")) {
                String id = reader.nextString();
                document.add(new DatasetField("dataset_id", id));
            } else if (Objects.equals(name, "title")) {
                String title = reader.nextString();
                document.add(new DatasetField("title", title));
            } else if (Objects.equals(name, "description")) {
                String description = reader.nextString();
                document.add(new DatasetField("description", description));
            } else if (Objects.equals(name, "author")) {
                String author = reader.nextString();
                document.add(new DatasetField("author", author));
            } else if (Objects.equals(name, "tags")) {
                //the tag are splitted and indexed
                String tags = reader.nextString();
                String[] tagsArray = tags.split(";");
                for (String tag : tagsArray) {
                    document.add(new DatasetField("tags", tag));
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
     * @param fileName name of the file
     * @return true if the file has a valid RDF extension
     */
    private boolean isRDFFile(String fileName){
        return Constants.suffixes.contains(FilenameUtils.getExtension(fileName));
    }

    /**
     * Indexes the datasets.
     *
     * @throws IOException if something goes wrong while indexing.
     */
    public void index() throws IOException {

        System.out.printf("%n#### Start indexing ####%n");

        FileWriter logFile = new FileWriter(Constants.logPath+"/indexer_log.txt", true);

        File[] datasetsDirectories = new File(datasetsDir.toString()).listFiles();

        int errors = 0;

        final int resume = 8908;
        HashSet<String> pass = new HashSet<>(Arrays.asList("dataset-11580"));
        int index = 1;

        for (File directory: datasetsDirectories) {

            //check the resume index and skip the datasets with uncorrect files
            if(index > resume && !pass.contains(directory.getName() )) {
                Document document = new Document();     //Lucene Document
                File[] files = directory.listFiles();

                int indexableFiles = files.length - 1; // Not count the dataset.json file

                for (File file : files) {

                    if (file.getName().equals("dataset.json")) {
                        //index the meta-data

                        //creating the JSON Parser
                        JsonReader reader = new JsonReader(new FileReader(file.toString()));

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
                                if(!indexMetaTags(reader, document, name))
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
                        if ((file.length() / (1024)^2) > 500){
                            logFile.write("Dataset: "+directory.getName()+"\nFile: "+file.getName()+"\nError: bigger than 500 MB");
                        } else {
                            //index the datasets content

                            bytesCount += file.getTotalSpace();

                            filesCount += 1;

                            RDFParser parser;
                            try {
                                parser = new RDFParser(file.getPath().toString());
                                while (parser.hasNext()) {
                                    RDFParser.CustomTriple triple = parser.next();

                                    document.add(new DatasetField(triple.getSubject().getKey(), triple.getSubject().getValue()));
                                    document.add(new DatasetField("properties", triple.getPredicate()));
                                    document.add(new DatasetField(triple.getObject().getKey(), triple.getObject().getValue()));
                                }
                                parser.close();
                            } catch (Exception e) {
                                logFile.write("Dataset: "+directory.getName()+"\nFile: "+file.getName()+"\nError: "+ e);
                                errors++;
                                indexableFiles--;
                            }
                        }
                    }
                }

                //TODO: update the dataset.json file with the indexable info

                //end of all datasets files scan
                writer.addDocument(document); //index the document

                datasetsCount++;

                //commit index after every 50 dataset for efficiency reasons
                //TODO: tune the parameter
                if (datasetsCount % 50 == 0) {
                    writer.commit();
                }

                // print progress every 10000 indexed documents, only for debug purpose
                if (datasetsCount % 100 == 0) {
                    System.out.printf("%d document(s) %d error(s) in parsing (%d files, %d Mbytes) indexed in %d seconds.%n",
                            datasetsCount, errors, filesCount, bytesCount / MBYTE,
                            (System.currentTimeMillis() - start) / 1000);
                }
            }
            index++;
        }

        //indexer commit and resource release
        writer.close();

        logFile.close();

        if (datasetsCount != expectedDatasets) {
            System.out.printf("Expected to index %d documents; %d indexed instead.%n", expectedDatasets, datasetsCount);
        }

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

        CharArraySet cas = AnalyzerUtil.loadStopList("nltk-stopwords.txt");
        final Analyzer a = new StandardAnalyzer(cas);

        //final Similarity sim = new LMDirichletSimilarity(1800);

        DatasetIndexer i = new DatasetIndexer(a, ramBuffer, indexPath, datasetDirectoryPath,
                charsetName, expectedDatasets);

        i.index();

    }

}
