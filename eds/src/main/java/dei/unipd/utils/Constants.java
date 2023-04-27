package dei.unipd.utils;

import java.util.Arrays;
import java.util.HashSet;

/**
 * This class contains the path for retrieving the files in our machines
 */
public class Constants {

    /**
     * ID of our runs
     */
    public static final String runID = "eds-2023";

    /**
     * Datasets directory
     */
    public static final String datasetsDirectoryPath = "/media/manuel/500GBHDD/Tesi/Datasets";

    public static final String datasetsDirectoryPathTest = "/home/manuel/Tesi/ACORDAR/Test";

    /**
     * Path to the index
     */
    public static final String indexPath = "/media/manuel/500GBHDD/Tesi/Index";

    public static final String indexPathTest = "/home/manuel/Tesi/ACORDAR/Index";

    /**
     * Path to the log files
     */
    public static final String logPath = "/home/manuel/Tesi/ACORDAR/Log";

    /**
     * Queries path
     */
    public static final String queryPath = "/home/manuel/Tesi/ACORDAR/Data/all_queries.txt";

    /**
     * Runs directory path
     */
    public static final String runPath = "/home/manuel/Tesi/ACORDAR/Run";

    public static String[] queryFields = {"title", "description", "author", "tags", "entities", "literals", "classes", "properties"};
    public static String[] snippetFields = {"author", "tags", "entities", "literals", "classes", "properties"};

    public static String[] methodList = {"BM25F", "BM25F [m]", "BM25F [d]", "TFIDF", "TFIDF [m]", "TFIDF [d]", "LMD", "LMD [m]", "LMD [d]", "FSDM", "FSDM [m]", "FSDM [d]", "DPR", "DPR [m]", "DPR [d]", "ColBERT", "ColBERT [m]", "ColBERT [d]"};

    // pooling weights
    public static float[] BM25BoostWeights = {1.0f, 0.9f, 0.9f, 0.6f, 0.2f, 0.3f, 0.1f, 0.1f};
    public static float[] TFIDFBoostWeights = {1.0f, 0.7f, 0.9f, 0.9f, 0.8f, 0.5f, 0.1f, 0.4f};
    public static float[] LMDBoostWeights = {1.0f, 0.9f, 0.1f, 1.0f, 0.2f, 0.3f, 0.2f, 0.1f};
    public static float[] FSDMBoostWeights = {1.0f, 0.1f, 0.5f, 0.9f, 0.1f, 0.1f, 0.4f, 0.6f};

    public static float[] BM25MetadataBoostWeights = {0.5f, 0.3f, 0.2f, 0.2f};
    public static float[] TFIDFMetadataBoostWeights = {1.0f, 0.6f, 0.4f, 0.5f};
    public static float[] LMDMetadataBoostWeights = {1.0f, 0.8f, 0.9f, 0.7f};
    public static float[] FSDMMetadataBoostWeights = {1.0f, 0.1f, 0.2f, 0.6f};

    public static float[] BM25ContentBoostWeights = {0.1f, 0.7f, 0.2f, 0.2f};
    public static float[] TFIDFContentBoostWeights = {0.3f, 1.0f, 0.6f, 0.3f};
    public static float[] LMDContentBoostWeights = {0.3f, 1.0f, 0.1f, 0.6f};
    public static float[] FSDMContentBoostWeights = {1.0f, 0.6f, 0.1f, 0.1f};

    public static final HashSet<String> suffixes= new HashSet<>(Arrays.asList("rdf", "rdfs", "ttl", "owl", "n3", "nt", "jsonld", "xml", "ntriples"));
}
