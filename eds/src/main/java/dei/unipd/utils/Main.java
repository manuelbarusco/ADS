package dei.unipd.utils;

import com.ctc.wstx.shaded.msv_core.verifier.jarv.Const;
import dei.unipd.analyze.AnalyzerUtil;
import dei.unipd.index.DatasetIndexer;
import dei.unipd.search.DatasetSearcher;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.en.EnglishMinimalStemFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.Similarity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import utils.Constants;

/**
 * Main clas that runs the whole EDS pipeline
 *
 * @author Riccardo Forzan
 * @author Manuel Barusco (manuel.barusco@studenti.unipd.it)
 */
public class Main {

    /**
     * Main method used to execute the whole
     *
     * @param args command line arguments (optional, not used)
     */
    public static void main(String[] args) {

        //Declare the constants of the run
        final String runID = Constants.runID;
        final String indexPath = Constants.indexPath;
        final String docsPath = Constants.datasetsDirectoryPath;
        final String topics = Constants.queryPath;
        final String runPath = Constants.runPath;

        final int ramBuffer = 256;
        final int maxDocsRetrieved = 1000;

        final int expectedDocs = 0; //TODO: to be modified
        final String charsetName = "ISO-8859-1";

        final int expectedTopics = 50; //TODO: to be modified

        CharArraySet cas = AnalyzerUtil.loadStopList("nltk-stopwords.txt");
        final Analyzer analyzer = new StandardAnalyzer(cas);

        Similarity similarity = new BM25Similarity();

        float[] weights = Constants.BM25BoostWeights;

        String[] fields = Constants.queryFields;
        Map<String,Float> boosts = new HashMap<>();
        for (int i = 0; i < fields.length; i ++) {
            boosts.put(fields[i], weights[i]);
        }

        //Read user's inputs
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Select an option: ");
        System.out.println("0 - Run the complete pipeline (indexer & searcher) (using BM25) to produce the run to submit");
        System.out.println("1 - Run the indexer only (using BM25)");
        System.out.println("2 - Run the searcher only (using BM25)");

        // Reading data using readLine
        Integer option = null;
        try {
            option = Integer.parseInt(reader.readLine());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        switch (option) {
            case 0 -> {
                runIndexer(analyzer, similarity, ramBuffer, indexPath, docsPath, charsetName, expectedDocs);
                runSearch(analyzer, similarity, indexPath, topics, expectedTopics, runID, runPath, maxDocsRetrieved, boosts);
            }
            case 1 -> runIndexer(analyzer, similarity, ramBuffer, indexPath, docsPath, charsetName, expectedDocs);
            case 2 -> runSearch(analyzer, similarity, indexPath, topics, expectedTopics, runID, runPath, maxDocsRetrieved, boosts);

        }

    }

    /**
     * run the indexing phase
     *
     * @param analyzer     analyzer that must be used
     * @param similarity   similarity that must be used
     * @param ramBuffer    dimension of the RAM buffer that must be used
     * @param indexPath    where to store the index files
     * @param docsPath     where to retrieve the collection documents
     * @param charsetName  charset to be used
     * @param expectedDocs number of documents expected to be retrieved
     */
    private static void runIndexer(Analyzer analyzer, Similarity similarity, int ramBuffer, String indexPath, String docsPath,
                                   String charsetName, int expectedDocs) {

        try {
            new DatasetIndexer(analyzer, ramBuffer, indexPath, docsPath,
                    charsetName, expectedDocs).index();
        } catch (IOException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }

    }

    /**
     * run the search phase
     *
     * @param analyzer         analyzer that must be used
     * @param similarity       similarity that must be used
     * @param indexPath        where the index files are stored
     * @param topics           where the topics file is stored
     * @param expectedTopics   number of expected topics
     * @param runID            id of the run
     * @param runPath          where to store the run results
     * @param maxDocsRetrieved maximum number of documents to be retrieved
     * @param queryWeights     weights to be used in the search boosting
     */
    private static void runSearch(Analyzer analyzer, Similarity similarity, String indexPath, String topics,
                                  int expectedTopics, String runID, String runPath, int maxDocsRetrieved, Map queryWeights) {

        DatasetSearcher s = new DatasetSearcher(analyzer, similarity, indexPath, topics, expectedTopics,
                runID, runPath, maxDocsRetrieved, queryWeights);

        //s.search();
        try {
            s.searchBoosted();
        } catch (IOException | ParseException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }

}
