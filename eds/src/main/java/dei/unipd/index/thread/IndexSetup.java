package dei.unipd.index.thread;

import dei.unipd.utils.Constants;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Manuel Barusco
 * @version 1.1
 *
 * This class is a simple setup class of the indexing phase.
 * This object will create the index writer object for indexing the datasets, a queue of
 * datasets that must be indexed and will launch all the indexing threads.
 */
public class IndexSetup {
    /**
     * One megabyte constant
     */
    private static final int MBYTE = 1024 * 1024;

    /**
     * Index Writer Configuration
     */
    private final IndexWriterConfig iwc;

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
     * Path to the error log file of the indexing phase
     */
    private final String logFilePath;

    /**
     * Creates a new indexing setup by defining the IndexWriterConfig and the paths
     *
     * @param analyzer                  the {@code Analyzer} to be used in the indexing phase
     * @param ramBufferSizeMB           the size in megabytes of the RAM buffer for indexing documents.
     * @param indexPath                 the directory where to store the index.
     * @param datasetsDirectoryPath     the directory from which datasets have to be read.
     * @param charsetName               the name of the charset used for encoding documents.
     * @param expectedDocs              the total number of datasets expected to be indexed
     * @param logFilePath               path to the error log file of the indexer
     * @throws NullPointerException     if any of the parameters is {@code null}.
     * @throws IllegalArgumentException if any of the parameters assumes invalid values.
     */
    public IndexSetup(final Analyzer analyzer, final int ramBufferSizeMB,
                          final String indexPath, final String datasetsDirectoryPath,
                          final String charsetName, final long expectedDocs, final String logFilePath) {

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
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE); //with this mode the indexer will create an index if it's not present or it will append the new values
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

        this.logFilePath = logFilePath;


    }

    /**
     * This method will create the datasets queue and will
     * start all the indexing threads. So it will also allocate all the needed structures.
     * @throws RuntimeException in case of exceptions during the initialization of the process
     */
    public void setupAndRunIndexing(){

        //create the queue of datasets
        File[] datasetsDirectories = new File(datasetsDir.toString()).listFiles();
        BlockingQueue<File> datasetsQueue = new LinkedBlockingQueue<>();
        datasetsQueue.addAll(Arrays.asList(datasetsDirectories));

        //create the IndexSharedInfo object
        IndexSharedInfo info;
        try {
            info = new IndexSharedInfo(indexDir, iwc, logFilePath);
        }catch (IOException e){
            throw new RuntimeException("Exception in the initialization of the IndexSharedInfo object");
        }

        System.out.printf("%n#### Start indexing ####%n");
        final long start = System.currentTimeMillis();

        //allocate and start the threads
        IndexerThread[] threads = new IndexerThread[2 ];
        for (int i=0; i<threads.length; i++){
            threads[i] = new IndexerThread(datasetsQueue, info);
            threads[i].start();
        }

        //wait for all the threads to stop
        for (int i=0; i<threads.length; i++){
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                throw new RuntimeException("Thread interrupted in an unexpected way");
            }
        }

        //close the open resources
        try {
            info.close();
        } catch (IOException e) {
            throw new RuntimeException("Error while releasing the shared info resources");
        }

        System.out.printf("%n#### End indexing ####%n");

        final long end = System.currentTimeMillis();

        System.out.println("Time: "+(end-start)/(1000*60)+" minutes");

        //log of the final stats

        System.out.println("Indexed datasets: "+info.getDatasetsCount()+"\nIndexed files: "+info.getFilesCount()+"\nErrors count: "+info.getErrorsCount());
    }

    /**
     * @author Manuel Barusco
     *
     * This class is a wrapper class for all the shared info that has to be
     * changed by the indexing thread during the indexing phase
     */
    public class IndexSharedInfo{

        /**
         * The Index Writer Lucene object
         */
        private final IndexWriter indexWriter;

        /**
         * The total number of indexed files (we count all the files inside the
         * datasets folder that are indexed except for the dataset.json file)
         */
        private long filesCount;

        /**
         * The total number of indexed datasets
         */
        private long datasetsCount;

        /**
         * The total number of indexed bytes
         */
        private long bytesCount;

        /**
         * Error log file
         */
        private FileWriter logFile;

        private long lastLog;
        private long errorsCount;

        /**
         * Constructor
         *
         * @param indexDir index directory Path object
         * @param iwc IndexWriterConfig
         * @param logFilePath path to the index log file
         */
        public IndexSharedInfo(Path indexDir, IndexWriterConfig iwc, String logFilePath) throws IOException {
            indexWriter = new IndexWriter(FSDirectory.open(indexDir), iwc);
            filesCount = 0;
            datasetsCount = 0;
            bytesCount = 0;
            lastLog = 0;
            errorsCount = 0;
            logFile = new FileWriter(logFilePath);
        }

        /**
         * This method will change the indexing information in a thread safe way
         * @param filesCountAdd increment of the filesCount
         * @param datasetsCountAdd increment of the datasetCount
         * @param bytesCountAdd increment of the bytesCount
         */
        public synchronized void add(long filesCountAdd, long datasetsCountAdd, long bytesCountAdd, long errorsCountAdd) throws IOException {
            filesCount += filesCountAdd;
            datasetsCount += datasetsCountAdd;
            bytesCount += bytesCountAdd;
            errorsCount += errorsCountAdd;

            if(datasetsCount - lastLog > 1 ) {
                System.out.println("Indexed: " +datasetsCount+" datasets with "+errorsCount+" errors");
                lastLog = datasetsCount;
            }

        }

        /**
         * This method will writer in the log file a given message
         * @param message to write in the log
         */
        public synchronized void logMessage(String message) throws IOException {
            logFile.write(message);
        }

        public long getErrorsCount(){
            return errorsCount;
        }

        public long getDatasetsCount(){
            return datasetsCount;
        }

        public long getFilesCount(){
            return filesCount;
        }

        public synchronized IndexWriter getIndexWriter(){
            return indexWriter;
        }

        /**
         * This method release all the resources: so the IndexWriter and the logFile
         */
        public void close() throws IOException {
            indexWriter.close();
            logFile.close();
        }

    }

}
