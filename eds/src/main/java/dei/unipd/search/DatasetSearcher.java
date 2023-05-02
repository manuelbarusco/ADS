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

package dei.unipd.search;

import dei.unipd.analyze.AnalyzerUtil;
import dei.unipd.parse.ParsedDataset;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.benchmark.quality.QualityQuery;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;
import dei.unipd.parse.CustomQueryParser;
import dei.unipd.utils.Constants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;


/**
 * Searches in a dataset collection
 *
 * @author Manuel Barusco (manuel.barusco@studenti.unipd.it)
 * @version 1.00
 * @since 1.00
 */
public class DatasetSearcher {

    /**
     * The identifier of the run
     */
    private final String runID;

    /**
     * The run to be written
     */
    private final PrintWriter run;

    /**
     * The index reader
     */
    private final IndexReader reader;

    /**
     * The index searcher.
     */
    private final IndexSearcher searcher;

    /**
     * The queries to be searched
     */
    private final QualityQuery[] topics;

    /**
     * The query parser
     */
    private final CustomQueryParser qp;

    /**
     * The maximum number of datasets to retrieve
     */
    private final int maxDatasetsRetrieved;

    /**
     * The total elapsed time.
     */
    private long elapsedTime = Long.MIN_VALUE;

    /**
     * New searcher.
     *
     * @param analyzer             the {@code Analyzer} to be used in the search phase
     * @param similarity           the {@code Similarity} to be used.
     * @param indexPath            the directory containing the index to be searched.
     * @param queryFile            the file containing the queries to search for.
     * @param expectedQueries      the total number of queries expected to be searched.
     * @param runID                the identifier of the run to be created.
     * @param runPath              the path where to store the run.
     * @param maxDatasetsRetrieved the maximum number of datasets to be retrieved.
     * @param queryWeights         fields weights for query boosting
     * @throws NullPointerException     if any of the parameters is {@code null}.
     * @throws IllegalArgumentException if any of the parameters assumes invalid values.
     */
    public DatasetSearcher(final Analyzer analyzer, final Similarity similarity, final String indexPath,
                          final String queryFile, final int expectedQueries, final String runID, final String runPath,
                          final int maxDatasetsRetrieved, Map<String, Float> queryWeights) {

        if (analyzer == null) {
            throw new NullPointerException("Analyzer cannot be null.");
        }

        if (similarity == null) {
            throw new NullPointerException("Similarity cannot be null.");
        }

        if (indexPath == null) {
            throw new NullPointerException("Index path cannot be null.");
        }

        if (indexPath.isEmpty()) {
            throw new IllegalArgumentException("Index path cannot be empty.");
        }

        final Path indexDir = Paths.get(indexPath);
        if (!Files.isReadable(indexDir)) {
            throw new IllegalArgumentException(String.format("Index directory %s cannot be read.", indexDir.toAbsolutePath()));
        }

        if (!Files.isDirectory(indexDir)) {
            throw new IllegalArgumentException(String.format("%s expected to be a directory where to search the index.",
                    indexDir.toAbsolutePath()));
        }

        try {
            reader = DirectoryReader.open(FSDirectory.open(indexDir));
        } catch (IOException e) {
            throw new IllegalArgumentException(String.format("Unable to create the index reader for directory %s: %s.",
                    indexDir.toAbsolutePath(), e.getMessage()), e);
        }

        //create the index searcher and set the similarity to use
        searcher = new IndexSearcher(reader);
        searcher.setSimilarity(similarity);

        if (queryFile == null) {
            throw new NullPointerException("Topics file cannot be null.");
        }

        if (queryFile.isEmpty()) {
            throw new IllegalArgumentException("Topics file cannot be empty.");
        }

        try {
            BufferedReader in = Files.newBufferedReader(Paths.get(queryFile), StandardCharsets.UTF_8);
            topics = new QueriesReader().readQueries(in);  //list of topics
            in.close();
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    String.format("Unable to process topic file %s: %s.", queryFile, e.getMessage()), e);
        }

        if (expectedQueries <= 0) {
            throw new IllegalArgumentException("The expected number of topics to be searched cannot be less than or equal to zero.");
        }

        if (topics.length != expectedQueries) {
            System.out.printf("Expected to search for %s topics; %s topics found instead.", expectedQueries, topics.length);
        }

        // Define different weights to different fields of the documents for applying query boosting
        // if the queryWeights parameter is null, set the default weights
        if (queryWeights == null || queryWeights.size() < 3) {
            queryWeights = new HashMap<>();
            queryWeights.put(ParsedDataset.FIELDS.TITLE, 2f);
            queryWeights.put(ParsedDataset.FIELDS.DESCRIPTION, 1f);
            queryWeights.put(ParsedDataset.FIELDS.TAGS, 1f);
            queryWeights.put(ParsedDataset.FIELDS.AUTHOR, 1f);
            queryWeights.put(ParsedDataset.FIELDS.ENTITIES, 1f);
            queryWeights.put(ParsedDataset.FIELDS.CLASSES, 1f);
            queryWeights.put(ParsedDataset.FIELDS.LITERALS, 1f);
            queryWeights.put(ParsedDataset.FIELDS.PROPERTIES, 1f);
        }

        qp = new CustomQueryParser(queryWeights, analyzer, ParsedDataset.FIELDS.DESCRIPTION);

        if (runID == null) {
            throw new NullPointerException("Run identifier cannot be null.");
        }

        if (runID.isEmpty()) {
            throw new IllegalArgumentException("Run identifier cannot be empty.");
        }

        this.runID = runID;

        if (runPath == null) {
            throw new NullPointerException("Run path cannot be null.");
        }

        if (runPath.isEmpty()) {
            throw new IllegalArgumentException("Run path cannot be empty.");
        }

        final Path runDir = Paths.get(runPath);
        if (!Files.isWritable(runDir)) {
            throw new IllegalArgumentException(String.format("Run directory %s cannot be written.", runDir.toAbsolutePath()));
        }

        if (!Files.isDirectory(runDir)) {
            throw new IllegalArgumentException(String.format("%s expected to be a directory where to write the run.",
                    runDir.toAbsolutePath()));
        }

        Path runFile = runDir.resolve(runID + ".txt");
        try {
            run = new PrintWriter(Files.newBufferedWriter(runFile, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE));
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    String.format("Unable to open run file %s: %s.", runFile.toAbsolutePath(), e.getMessage()), e);
        }

        if (maxDatasetsRetrieved <= 0) {
            throw new IllegalArgumentException("The maximum number of documents to be retrieved cannot be less than or equal to zero.");
        }

        this.maxDatasetsRetrieved = maxDatasetsRetrieved;
    }

    /**
     * Main method just to for testing purposes
     *
     * @param args command line arguments.
     * @throws Exception if something goes wrong while indexing.
     */
    public static void main(String[] args) throws Exception {

        final String runID = Constants.runID;
        final String indexPath = Constants.indexPath;
        final String datasetsPath = Constants.datasetsDirectoryPath;
        final String queriesPath = Constants.queryPath;
        final String runPath = Constants.runPath;

        final int maxDocsRetrieved = 1000;

        //setting the query boosting weights
        Map<String, Float> queryWeights = new HashMap<>();
        queryWeights.put(ParsedDataset.FIELDS.TITLE, 4f);
        queryWeights.put(ParsedDataset.FIELDS.DESCRIPTION, 2f);
        queryWeights.put(ParsedDataset.FIELDS.AUTHOR, 2f);
        queryWeights.put(ParsedDataset.FIELDS.TAGS, 2f);
        queryWeights.put(ParsedDataset.FIELDS.CLASSES, 4f);
        queryWeights.put(ParsedDataset.FIELDS.ENTITIES, 2f);
        queryWeights.put(ParsedDataset.FIELDS.LITERALS, 2f);
        queryWeights.put(ParsedDataset.FIELDS.PROPERTIES, 2f);

        //ACORDAR settings
        CharArraySet cas = AnalyzerUtil.loadStopList("nltk-stopwords.txt");
        final Analyzer a = new StandardAnalyzer(cas);

        final Similarity sim = new LMDirichletSimilarity(1800);

        DatasetSearcher s = new DatasetSearcher(a, sim, indexPath, queriesPath, 50, runID, runPath, maxDocsRetrieved, queryWeights);

        s.search();

    }

    /**
     * Returns the total elapsed time.
     *
     * @return the total elapsed time.
     */
    public long getElapsedTime() {
        return elapsedTime;
    }

    /**
     * Searches for the specified queries without Query Boosting and without Query Expansion and Re-Ranking
     * this is a base method for searching
     *
     * @throws IOException    if something goes wrong while searching.
     * @throws ParseException if something goes wrong while parsing topics.
     */
    public void search() throws IOException, ParseException {

        System.out.printf("%n#### Start searching ####%n");

        // the start time of the searching
        final long start = System.currentTimeMillis();

        //fields that must be retrieved from the system
        final Set<String> fieldsToBeRetrieved = new HashSet<>();
        fieldsToBeRetrieved.add(ParsedDataset.FIELDS.ID);

        BooleanQuery.Builder bq;
        Query q;
        TopDocs docs;
        ScoreDoc[] sd;
        String docID; //document ID

        try (PrintWriter runDefault = new PrintWriter(Constants.runPath + "/" + runID + ".txt")) {

            for (QualityQuery t : topics) {

                System.out.printf("Searching for topic %s.%n", t.getQueryID());

                //original query
                bq = new BooleanQuery.Builder();

                bq.add(qp.parse(QueryParserBase.escape(t.getValue(QUERY_FIELDS.TEXT))), BooleanClause.Occur.SHOULD);

                //Check the text field is not null/empty/blank
                String text = t.getValue(QUERY_FIELDS.TEXT);
                if (text != null && !text.isEmpty() && !text.isBlank())
                    bq.add(qp.parse(QueryParserBase.escape(text)), BooleanClause.Occur.SHOULD);

                q = bq.build();

                docs = searcher.search(q, maxDatasetsRetrieved);

                sd = docs.scoreDocs;

                //HashSet for removing duplicated document IDs in the search
                Set<String> nod = new HashSet<>();

                for (int i = 0, n = sd.length; i < n; i++) {

                    docID = reader.document(sd[i].doc, fieldsToBeRetrieved).get(ParsedDataset.FIELDS.ID);
                    if (!nod.contains(docID)) {
                        nod.add(docID);
                    }

                    //write the search results in the runDefault output file (in the standard TREC format)
                    runDefault.printf(Locale.ENGLISH, "%s\tQ0\t%s\t%d\t%.6f\t%s%n", t.getQueryID(), docID, i, sd[i].score, runID);
                    i++;
                }

                run.flush();
                runDefault.flush();

            }
        } finally {
            run.close();
            reader.close();
        }

        elapsedTime = System.currentTimeMillis() - start;

        System.out.printf("%d topic(s) searched in %d seconds.\n", topics.length, elapsedTime / 1000);

        System.out.print("#### Searching complete ####\n");
    }

    /**
     * TO BE DONE, it's only a template
     *
     * Searches for the specified topics using:
     * <ol>
     * <li>Query Boosting with the specified weight parameters</li>
     * <li>Query Expansion with the specified score threshold for synonyms</li>
     * <li>Re ranking based on sentiment analysis and readability of the document</li>
     * </ol>
     *
     * @param qExp               boolean that indicates if we want to use query expansion
     * @param reSent             boolean that indicates if we want to use re rank based on sentiment analysis on the document conclusion
     * @param reRead             boolean that indicates if we want to use re rank based on readability of the document conclusion
     * @param allTokens          boolean parameter that indicates if we want to generate synonyms for every token or only for the main token
     * @param maxSynonymsPerWord number of synonyms to generate for every key token
     * @param threshold          score threshold used in query expansion
     * @throws IOException    if something goes wrong while searching.
     * @throws ParseException if something goes wrong while parsing topics.
     */
    public void searchBoostedAugmented(boolean qExp, boolean reSent, boolean reRead,
                              boolean allTokens, int maxSynonymsPerWord, double threshold) throws IOException, ParseException {

        System.out.printf("%n#### Start boosted searching ####%n");

        // the start time of the searching
        final long start = System.currentTimeMillis();

        //fields that must be retrieved from the system
        final Set<String> idField = new HashSet<>();
        idField.add(ParsedDataset.FIELDS.ID);

        BooleanQuery.Builder bq;
        Query q;
        TopDocs docs;
        ScoreDoc[] sd;
        String docID; //document ID
        String[] sentencesID; //sentences ID of the document
        String stance; //stance of the document

        Query titleQuery;
        Query descriptionQuery;

        /*
         * only for tuning the system, we write the results of the search in 2 different file
         * runDefault: file that contains the results of the search in a Standard TREC format,
         * it can be parsed from trec_eval. We use this file for parameter tuning and test the different solutions.
         * run: file that contains the results of the search with the sentence pairs that we have to submit to CLEF
         */
        try (PrintWriter runDefault = new PrintWriter(Constants.runPath + "/" + runID + ".txt")) {

            for (QualityQuery t : topics) {

                System.out.printf("Searching for topic %s.%n", t.getQueryID());

                //Perform the original query
                bq = new BooleanQuery.Builder();
                titleQuery = qp.multipleFieldsParse(t.getValue(QUERY_FIELDS.TEXT));
                bq.add(titleQuery, BooleanClause.Occur.SHOULD);

                //Check the description field is not null/empty/blank
                String text = t.getValue(QUERY_FIELDS.TEXT);
                if (text != null && !text.isEmpty() && !text.isBlank()) {
                    descriptionQuery = qp.multipleFieldsParse(text);
                    bq.add(descriptionQuery, BooleanClause.Occur.SHOULD);
                }

                //Execute the original query
                q = bq.build();
                docs = searcher.search(q, maxDatasetsRetrieved);
                sd = docs.scoreDocs;

                //Add the documents found to the result
                ArrayList<ScoreDoc> documents = new ArrayList<>(Arrays.asList(sd));

                /*
                //Check if we have to use query expansion
                if (qExp) {

                    //Get the expanded queries (removes duplicated queries eventually)
                    List<String> expandedQueries = QueryExpander.generateAllExpandedQueries(t.getValue(TOPIC_FIELDS.TITLE), allTokens, maxSynonymsPerWord, threshold)
                            .stream().distinct().toList();

                    //Iterate over all the expanded queries and execute them
                    for (String titleString : expandedQueries) {
                        System.out.printf("Expanded query: %s\n", titleString);

                        bq = new BooleanQuery.Builder();
                        titleQuery = qp.multipleFieldsParse(titleString);
                        bq.add(titleQuery, BooleanClause.Occur.SHOULD);

                        //Check the description field is not null
                        description = t.getValue(TOPIC_FIELDS.DESCRIPTION);
                        if (description != null && !description.isEmpty() && !description.isBlank()) {
                            descriptionQuery = qp.multipleFieldsParse(description);
                            bq.add(descriptionQuery, BooleanClause.Occur.SHOULD);
                        }

                        //Build the query
                        q = bq.build();
                        docs = searcher.search(q, maxDocsRetrieved);
                        sd = docs.scoreDocs;

                        //Add all the retrieved documents to the array list
                        documents.addAll(Arrays.asList(sd));
                    }
                }


                //check if the results must be re-ranked based on sentiment analysis
                List<ScoreDoc> sentimentOrder = null;
                if (reSent) {
                    //Re ranking based on sentiment analysis
                    Ranker sentimentRanker = new Ranker(reader, t, documents);
                    sentimentOrder = sentimentRanker.rankUsingSentiment();
                }

                //check if the results must be re-ranked based on readability of the document text
                List<ScoreDoc> readabilityOrder = null;
                if (reRead && !reSent) {
                    Ranker readabilityRanker = new Ranker(reader, t, new ArrayList<>(documents));
                    readabilityOrder = readabilityRanker.rankByReadability();
                } else if (reRead) {
                    Ranker readabilityRanker = new Ranker(reader, t, new ArrayList<>(sentimentOrder));
                    readabilityOrder = readabilityRanker.rankByReadability();
                }

                 */

                //Sorting the retrieved documents by their score and cut the list to maxDocsRetrieved
                List<ScoreDoc> cutUniqueDocuments = null;
                if (!reSent && !reRead) {
                    documents.sort((o1, o2) -> Float.compare(o1.score, o2.score));
                    Collections.reverse(documents);
                    cutUniqueDocuments = documents.subList(0, maxDatasetsRetrieved);
                }

                /*else if (reSent && !reRead) {
                    sentimentOrder.sort((o1, o2) -> Float.compare(o1.score, o2.score));
                    Collections.reverse(sentimentOrder);
                    cutUniqueDocuments = sentimentOrder.subList(0, maxDocsRetrieved);
                } else {
                    readabilityOrder.sort((o1, o2) -> Float.compare(o1.score, o2.score));
                    Collections.reverse(readabilityOrder);
                    cutUniqueDocuments = readabilityOrder.subList(0, maxDocsRetrieved);
                }
                */

                //print the results
                int i = 1;
                int pairsCounter = 1;
                //HasSet for removing duplicated document IDs in the search
                HashSet<String> docIDs = new HashSet<>();

                for (ScoreDoc document : cutUniqueDocuments) {

                    //retrieve the docID
                    docID = reader.document(document.doc, idField).get(ParsedDataset.FIELDS.ID);

                    //check if the docID was already retrieve
                    if (!docIDs.contains(docID)) {
                        docIDs.add(docID);

                        //write the search results in the runDefault output file (in the standard TREC format)
                        runDefault.printf(Locale.ENGLISH, "%s\tQ0\t%s\t%d\t%.6f\t%s%n", t.getQueryID(), docID, i++, document.score, runID);
                    }
                }
                run.flush();
                runDefault.flush();
            }
        } finally {
            run.close();
            reader.close();
        }

        elapsedTime = System.currentTimeMillis() - start;
        System.out.printf("%d topic(s) searched in %d seconds.\n", topics.length, elapsedTime / 1000);
        System.out.print("#### Searching complete ####\n");
    }

    /**
     * Searches for the specified queries with boosting applied
     *
     * @throws IOException    if something goes wrong while searching.
     * @throws ParseException if something goes wrong while parsing topics.
     */
    public void searchBoosted() throws IOException, ParseException {

        System.out.printf("%n#### Start boosted searching ####%n");

        // the start time of the searching
        final long start = System.currentTimeMillis();

        //fields that must be retrieved from the system
        final Set<String> fieldsToLoad = new HashSet<>();
        fieldsToLoad.add(ParsedDataset.FIELDS.ID);

        Query q;
        TopDocs docs;
        ScoreDoc[] sd;
        String docID; //document ID

        try (PrintWriter runDefault = new PrintWriter(Constants.runPath + "/" + runID + ".txt")) {

            for (QualityQuery t : topics) {

                System.out.printf("Searching for topic %s.%n", t.getQueryID());

                //Execute the original query
                q = qp.multipleFieldsParse(t.getValue(QUERY_FIELDS.TEXT));

                docs = searcher.search(q, maxDatasetsRetrieved);
                sd = docs.scoreDocs;

                //Add the documents found to the result
                ArrayList<ScoreDoc> documents = new ArrayList<>(Arrays.asList(sd));

                //print the results
                int i = 1;

                //HasSet for removing duplicated document IDs in the search
                HashSet<String> docIDs = new HashSet<>();

                for (ScoreDoc document : documents) {

                    //retrieve the docID
                    docID = reader.document(document.doc, fieldsToLoad).get(ParsedDataset.FIELDS.ID);

                    //check if the docID was already retrieve
                    if (!docIDs.contains(docID)) {
                        docIDs.add(docID);

                        //write the search results in the runDefault output file (in the standard TREC format)
                        runDefault.printf(Locale.ENGLISH, "%s\tQ0\t%s\t%d\t%.6f\t%s%n", t.getQueryID(), docID, i++, document.score, runID);
                    }
                }
                run.flush();
                runDefault.flush();
            }
        } finally {
            run.close();
            reader.close();
        }

        elapsedTime = System.currentTimeMillis() - start;
        System.out.printf("%d topic(s) searched in %d seconds.\n", topics.length, elapsedTime / 1000);
        System.out.print("#### Searching complete ####\n");
    }

    /**
     * The fields of the typical TREC topics.
     *
     * @author Nicola Rizzetto
     * @version 1.00
     * @since 1.00
     */
    public static final class QUERY_FIELDS {

        /**
         * The title of a topic.
         */
        public static final String ID = "query_id";

        /**
         * The description of a topic.
         */
        public static final String TEXT = "description";

    }

}



