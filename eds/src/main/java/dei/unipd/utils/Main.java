package utils;

import analyze.ToucheAnalyzer;
import analyze.ToucheAnalyzerTuning;
import index.ToucheIndexer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishMinimalStemFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import parse.ParsedDocument;
import parse.ToucheParser;
import search.ToucheSearcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Main clas that runs the whole project
 *
 * @author Riccardo Forzan
 * @author Manuel Barusco (manuel.barusco@studenti.unipd.it)
 */
public class Main {

    /**
     * Main method used to execute the while pipeline of the project
     *
     * @param args command line arguments (optional, not used)
     */
    public static void main(String[] args) {

        //Declare the constants of the run
        final String runID = Constants.runID + "NRR_QE_";
        final String indexPath = Constants.riccardoIndexPath;
        final String docsPath = Constants.riccardoDocsPath;
        final String topics = Constants.riccardoTopicsPath;
        final String runPath = Constants.riccardoRunPath;

        final int ramBuffer = 256;
        final int maxDocsRetrieved = 1000;

        final String extension = "csv";
        final int expectedDocs = 365408;
        final String charsetName = "ISO-8859-1";

        final int expectedTopics = 50;
        final Analyzer analyzer = new ToucheAnalyzer();
        Similarity similarity = new BM25Similarity();

        Map<String, Float> queryWeights = new HashMap<>();
        queryWeights.put(ParsedDocument.FIELDS.SOURCE_TEXT, 2f);
        queryWeights.put(ParsedDocument.FIELDS.CONCLUSION, 1f);
        queryWeights.put(ParsedDocument.FIELDS.DISCUSSION_TITLE, 1f);
        queryWeights.put(ParsedDocument.FIELDS.SOURCE_TITLE, 1f);

        //Read user's inputs
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Select an option: ");
        System.out.println("0 - Run the complete pipeline (indexer & searcher) (using BM25) to produce the run to submit");
        System.out.println("1 - Run the indexer only (using BM25)");
        System.out.println("2 - Run the searcher only (using BM25)");
        System.out.println("3 - Run indexer & searcher with different stoplists (using BM25)");
        System.out.println("4 - Run indexer & searcher with different stemmers (using BM25)");
        System.out.println("5 - Run the whole pipeline with different score thresholds in query expansion (using BM25)");
        System.out.println("6 - Run searcher with different score thresholds in query expansion (using LMDirichletSimilarity)");

        // Reading data using readLine
        Integer option = null;
        try {
            option = Integer.parseInt(reader.readLine());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        switch (option) {
            case 0 -> {
                runIndexer(analyzer, similarity, ramBuffer, indexPath, docsPath, extension, charsetName, expectedDocs);
                runSearch(analyzer, similarity, indexPath, topics, expectedTopics, runID, runPath, maxDocsRetrieved, queryWeights);
            }
            case 1 -> runIndexer(analyzer, similarity, ramBuffer, indexPath, docsPath, extension, charsetName, expectedDocs);
            case 2 -> runSearch(analyzer, similarity, indexPath, topics, expectedTopics, runID, runPath, maxDocsRetrieved, queryWeights);
            case 3 -> runIndexerAndSearcherStopList(ramBuffer, indexPath, docsPath, extension, charsetName, expectedDocs, topics, expectedTopics, runID, runPath, maxDocsRetrieved, queryWeights);
            case 4 -> runIndexerAndSearcherStemmer(null, ramBuffer, indexPath, docsPath, extension, charsetName, expectedDocs, topics, expectedTopics, runID, runPath, maxDocsRetrieved, queryWeights);
            case 5 -> {
                runIndexer(analyzer, similarity, ramBuffer, indexPath, docsPath, extension, charsetName, expectedDocs);
                runSearchDifferentThresholdsQE(analyzer, similarity, indexPath, topics, expectedTopics, runID, runPath, maxDocsRetrieved, queryWeights);
            }
            case 6 -> {
                similarity = new LMDirichletSimilarity();
                runIndexer(analyzer, similarity, ramBuffer, indexPath, docsPath, extension, charsetName, expectedDocs);
                runSearchDifferentThresholdsQE(analyzer, similarity, indexPath, topics, expectedTopics, runID, runPath, maxDocsRetrieved, queryWeights);
            }

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
     * @param extension    extension of the document files
     * @param charsetName  charset to be used
     * @param expectedDocs number of documents expected to be retrieved
     */
    private static void runIndexer(Analyzer analyzer, Similarity similarity, int ramBuffer, String indexPath, String docsPath,
                                   String extension, String charsetName, int expectedDocs) {

        try {
            new ToucheIndexer(analyzer, similarity, ramBuffer, indexPath, docsPath, extension,
                    charsetName, expectedDocs, ToucheParser.class).index();
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

        ToucheSearcher s = new ToucheSearcher(analyzer, similarity, indexPath, topics, expectedTopics,
                runID, runPath, maxDocsRetrieved, queryWeights);

        //s.search();
        try {
            s.searchBoosted(true, false, false, false, 100, 0.5);
        } catch (IOException | ParseException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }

    /**
     * run indexing and search phases with different stoplists for system tuning
     *
     * @param ramBuffer        dimension of the RAM buffer that must be used
     * @param indexPath        where to store the index files
     * @param docsPath         where to retrieve the collection documents
     * @param extension        extension of the document files
     * @param charsetName      charset to be used
     * @param expectedDocs     number of documents expected to be retrieved
     * @param topics           where the topics file is stored
     * @param expectedTopics   number of expected topics
     * @param runID            id of the run
     * @param runPath          where to store the run results
     * @param maxDocsRetrieved maximum number of documents to be retrieved
     * @param queryWeights     weights to be used in the search boosting
     */
    private static void runIndexerAndSearcherStopList(int ramBuffer, String indexPath, String docsPath,
                                                      String extension, String charsetName, int expectedDocs, String topics,
                                                      int expectedTopics, String runID, String runPath, int maxDocsRetrieved, Map queryWeights) {

        //trying different stop lists
        ArrayList<String> stoplists = new ArrayList<>();
        stoplists.add("smart.txt");
        stoplists.add("ebsco.txt");
        stoplists.add("corenlp.txt");
        stoplists.add("google.txt");
        stoplists.add("ranks.txt");
        stoplists.add("countwordsfree.txt");

        for (String list : stoplists) {
            //indexing with that stoplist and try to do a search
            try {
                System.out.println(list);
                ToucheAnalyzerTuning analyzer = new ToucheAnalyzerTuning(list, null);
                Similarity similarity = new LMDirichletSimilarity(1800);
                new ToucheIndexer(analyzer, similarity, ramBuffer, indexPath, docsPath, extension,
                        charsetName, expectedDocs, ToucheParser.class).index();
                new ToucheSearcher(analyzer, similarity, indexPath, topics, expectedTopics,
                        runID + "_" + similarity + "_" + list, runPath, maxDocsRetrieved, queryWeights).search();
            } catch (IOException | ParseException e) {
                System.out.println(e.getMessage());
                System.exit(1);
            }
        }

    }

    /**
     * run indexing and search phases with different stemmers for system tuning
     *
     * @param stopList         stoplist to be used
     * @param ramBuffer        dimension of the RAM buffer that must be used
     * @param indexPath        where to store the index files
     * @param docsPath         where to retrieve the collection documents
     * @param extension        extension of the document files
     * @param charsetName      charset to be used
     * @param expectedDocs     number of documents expected to be retrieved
     * @param topics           where the topics file is stored
     * @param expectedTopics   number of expected topics
     * @param runID            id of the run
     * @param runPath          where to store the run results
     * @param maxDocsRetrieved maximum number of documents to be retrieved
     * @param queryWeights     weights to be used in the search boosting
     */
    private static void runIndexerAndSearcherStemmer(String stopList, int ramBuffer, String indexPath, String docsPath,
                                                     String extension, String charsetName, int expectedDocs, String topics,
                                                     int expectedTopics, String runID, String runPath, int maxDocsRetrieved, Map queryWeights) {

        //trying different stemmers
        ArrayList<Class> stemmers = new ArrayList<>();
        stemmers.add(EnglishMinimalStemFilter.class);
        stemmers.add(KStemFilter.class);
        stemmers.add(PorterStemFilter.class);

        for (Class stemmer : stemmers) {
            //indexing with that stemmer and try to do a search
            try {
                ToucheAnalyzerTuning analyzer = new ToucheAnalyzerTuning(stopList, null);
                Similarity similarity = new LMDirichletSimilarity(1800);
                new ToucheIndexer(analyzer, similarity, ramBuffer, indexPath, docsPath, extension,
                        charsetName, expectedDocs, ToucheParser.class).index();
                new ToucheSearcher(analyzer, similarity, indexPath, topics, expectedTopics,
                        runID + "_" + similarity + "_" + stopList + "_" + stemmer.getName(), runPath, maxDocsRetrieved, queryWeights).search();
            } catch (IOException | ParseException e) {
                System.out.println(e.getMessage());
                System.exit(1);
            }
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
    private static void runSearchDifferentThresholdsQE(Analyzer analyzer, Similarity similarity, String indexPath,
                                                       String topics, int expectedTopics, String runID, String runPath,
                                                       int maxDocsRetrieved, Map queryWeights) {


        double[] thresholds = {0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 1.00};
        int[] maxSynonymsPerWordValues = {1, 2, 5, 7, 10, 100};
        boolean[] allTokensValues = {true, false};

        for (boolean allTokens : allTokensValues) {
            for (int maxSynonymsPerWord : maxSynonymsPerWordValues) {
                for (double threshold : thresholds) {

                    String rID = String.format("%s_allTokens_%s_maxSynonyms_%d_threshold_%.2f", runID,
                            allTokens, maxSynonymsPerWord, threshold);

                    ToucheSearcher s = new ToucheSearcher(analyzer, similarity, indexPath, topics, expectedTopics,
                            rID, runPath, maxDocsRetrieved, queryWeights);

                    try {

                        s.searchBoosted(true, false, false, allTokens, maxSynonymsPerWord, threshold);

                    } catch (IOException | ParseException e) {
                        System.out.println(e.getMessage());
                        System.exit(1);
                    }
                }
            }
        }

    }

}
