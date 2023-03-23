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

package search;

import analyze.ToucheAnalyzerQueries;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.benchmark.quality.QualityQuery;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;
import parse.CustomQueryParser;
import parse.ParsedDocument;
import utils.Constants;

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
 * Searches a document collection.
 *
 * @author Nicola Rizzetto (nicola.rizzetto.2@studenti.unipd.it)
 * @author Elham Soleymani (elham.soleymani@studenti.unipd.it)
 * @author Manuel Barusco (manuel.barusco@studenti.unipd.it)
 * @author Riccardo Forzan (riccardo.forzan@studenti.unipd.it)
 * @version 1.00
 * @since 1.00
 */
public class ToucheSearcher {

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
     * The topics to be searched
     */
    private final QualityQuery[] topics;
    /**
     * The query parser
     */
    private final CustomQueryParser qp;
    /**
     * The maximum number of documents to retrieve
     */
    private final int maxDocsRetrieved;
    /**
     * The total elapsed time.
     */
    private long elapsedTime = Long.MIN_VALUE;

    /**
     * New searcher.
     *
     * @param analyzer         the {@code Analyzer} to be used.
     * @param similarity       the {@code Similarity} to be used.
     * @param indexPath        the directory containing the index to be searched.
     * @param topicsFile       the file containing the topics to search for.
     * @param expectedTopics   the total number of topics expected to be searched.
     * @param runID            the identifier of the run to be created.
     * @param runPath          the path where to store the run.
     * @param maxDocsRetrieved the maximum number of documents to be retrieved.
     * @param queryWeights     fields weight for query boosting
     * @throws NullPointerException     if any of the parameters is {@code null}.
     * @throws IllegalArgumentException if any of the parameters assumes invalid values.
     */
    public ToucheSearcher(final Analyzer analyzer, final Similarity similarity, final String indexPath,
                          final String topicsFile, final int expectedTopics, final String runID, final String runPath,
                          final int maxDocsRetrieved, Map<String, Float> queryWeights) {

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

        searcher = new IndexSearcher(reader);
        searcher.setSimilarity(similarity);

        if (topicsFile == null) {
            throw new NullPointerException("Topics file cannot be null.");
        }

        if (topicsFile.isEmpty()) {
            throw new IllegalArgumentException("Topics file cannot be empty.");
        }

        try {
            BufferedReader in = Files.newBufferedReader(Paths.get(topicsFile), StandardCharsets.UTF_8);
            topics = new ToucheTopicsReader().readQueries(in);  //list of topics
            in.close();
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    String.format("Unable to process topic file %s: %s.", topicsFile, e.getMessage()), e);
        }

        if (expectedTopics <= 0) {
            throw new IllegalArgumentException("The expected number of topics to be searched cannot be less than or equal to zero.");
        }

        if (topics.length != expectedTopics) {
            System.out.printf("Expected to search for %s topics; %s topics found instead.", expectedTopics, topics.length);
        }

        // Define different weights to different fields of the documents for applying query boosting
        // if the queryWeights parameter is null, set the default weights
        if (queryWeights == null || queryWeights.size() < 3) {
            queryWeights = new HashMap<>();
            queryWeights.put(ParsedDocument.FIELDS.SOURCE_TEXT, 2f);
            queryWeights.put(ParsedDocument.FIELDS.CONCLUSION, 1f);
            queryWeights.put(ParsedDocument.FIELDS.DISCUSSION_TITLE, 1f);
            queryWeights.put(ParsedDocument.FIELDS.SOURCE_TITLE, 1f);
        }

        qp = new CustomQueryParser(queryWeights, analyzer, ParsedDocument.FIELDS.SOURCE_TEXT);

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

        if (maxDocsRetrieved <= 0) {
            throw new IllegalArgumentException("The maximum number of documents to be retrieved cannot be less than or equal to zero.");
        }

        this.maxDocsRetrieved = maxDocsRetrieved;
    }

    /**
     * Main method just to for testing purposes
     *
     * @param args command line arguments.
     * @throws Exception if something goes wrong while indexing.
     */
    public static void main(String[] args) throws Exception {

        final String runID = Constants.runID;
        final String indexPath = Constants.manuelIndexPath;
        final String docsPath = Constants.manuelDocsPath;
        final String topics = Constants.manuelTopics2021;
        final String runPath = Constants.manuelRunPath;

        final int maxDocsRetrieved = 1000;

        Map<String, Float> queryWeights = new HashMap<>();
        queryWeights.put(ParsedDocument.FIELDS.SOURCE_TEXT, 4f);
        queryWeights.put(ParsedDocument.FIELDS.CONCLUSION, 2f);
        queryWeights.put(ParsedDocument.FIELDS.DISCUSSION_TITLE, 2f);
        queryWeights.put(ParsedDocument.FIELDS.SOURCE_TITLE, 2f);

        final Analyzer a = new ToucheAnalyzerQueries();
        final Similarity sim = new LMDirichletSimilarity(1800);

        ToucheSearcher s = new ToucheSearcher(a, sim, indexPath, topics, 50, runID, runPath, maxDocsRetrieved, queryWeights);

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
     * Searches for the specified topics without Query Boosting and without Query Expansion and Re-Ranking
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
        final Set<String> idField = new HashSet<>();
        idField.add(ParsedDocument.FIELDS.ID);
        idField.add(ParsedDocument.FIELDS.SENTENCES);
        idField.add(ParsedDocument.FIELDS.STANCE);

        BooleanQuery.Builder bq;
        Query q;
        TopDocs docs;
        ScoreDoc[] sd;
        String docID; //document ID
        String[] sentencesID; //sentences ID of the document
        String stance; //stance of the document

        /* only for tuning the system, we write the results of the search in 2 different file
         * runDefault: file that contains the results of the search in a Standard TREC format, it can be parse from trec_eval.
         * We use this file for parameter tuning and test the different solutions.
         * run: file that contains the results of the search with the sentence pairs that we have to submit to CLEF
         */

        try (PrintWriter runDefault = new PrintWriter(Constants.riccardoTRECEvalFile + "_" + runID + ".txt")) {

            for (QualityQuery t : topics) {

                System.out.printf("Searching for topic %s.%n", t.getQueryID());

                //original query
                bq = new BooleanQuery.Builder();

                bq.add(qp.parse(QueryParserBase.escape(t.getValue(TOPIC_FIELDS.TITLE))), BooleanClause.Occur.SHOULD);

                //Check the description field is not null/empty/blank
                String description = t.getValue(TOPIC_FIELDS.DESCRIPTION);
                if (description != null && !description.isEmpty() && !description.isBlank())
                    bq.add(qp.parse(QueryParserBase.escape(description)), BooleanClause.Occur.SHOULD);

                q = bq.build();

                docs = searcher.search(q, maxDocsRetrieved);

                sd = docs.scoreDocs;

                //HasSet for removing duplicated document IDs in the search
                Set<String> nod = new HashSet<>();

                //HasSet for removing duplicated sentences pair in the search
                Set<String> stanceAndSentencesIDRetrieved = new HashSet<>();

                for (int i = 0, n = sd.length; i < n; i++) {

                    docID = reader.document(sd[i].doc, idField).get(ParsedDocument.FIELDS.ID);
                    if (!nod.contains(docID)) {
                        nod.add(docID);
                        sentencesID = reader.document(sd[i].doc, idField).getValues(ParsedDocument.FIELDS.SENTENCES);
                        stance = reader.document(sd[i].doc, idField).get(ParsedDocument.FIELDS.STANCE);

                        //prepare premises and sentences id for the ToucheSentencesRetriever
                        Vector<String> premsIDs = new Vector<>();
                        Vector<String> conclIDs = new Vector<>();
                        for (String s : sentencesID) {
                            if (s.contains("PREM"))
                                premsIDs.add(s);
                            else if (s.contains("CONC"))
                                conclIDs.add(s);
                        }

                        //if the argument has no conclusion sentence id, add a custom one
                        if (conclIDs.isEmpty())
                            conclIDs.add(docID + "__CONC__1");

                        //retrieve the sentences pairs
                        ToucheSentencesRetriever sentRetr = new ToucheSentencesRetriever(premsIDs, conclIDs);

                        //write the sentences in the run output file (in the format required by CLEF)
                        for (String[] pair : sentRetr) {
                            //create the sentences pair
                            String sentencesPair = String.format("%s\s%s\s%s,%s\s", t.getQueryID(), stance, pair[0], pair[1]);

                            //check if the sentences pair was already retrieved
                            if (!stanceAndSentencesIDRetrieved.contains(sentencesPair)) {
                                run.printf(Locale.ENGLISH, "%s\s%d\s%.2f\s%s%n", sentencesPair, i, sd[i].score, runID);
                                stanceAndSentencesIDRetrieved.add(sentencesPair);
                            }
                        }

                        //write the search results in the runDefault output file (in the standard TREC format)
                        runDefault.printf(Locale.ENGLISH, "%s\tQ0\t%s\t%d\t%.6f\t%s%n", t.getQueryID(), docID, i, sd[i].score, runID);
                        i++;
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
    public void searchBoosted(boolean qExp, boolean reSent, boolean reRead,
                              boolean allTokens, int maxSynonymsPerWord, double threshold) throws IOException,
            ParseException {

        System.out.printf("%n#### Start boosted searching ####%n");

        // the start time of the searching
        final long start = System.currentTimeMillis();

        //fields that must be retrieved from the system
        final Set<String> idField = new HashSet<>();
        idField.add(ParsedDocument.FIELDS.ID);
        idField.add(ParsedDocument.FIELDS.SENTENCES);
        idField.add(ParsedDocument.FIELDS.STANCE);

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
        try (PrintWriter runDefault = new PrintWriter(Constants.riccardoTRECEvalFile + "_" + runID + ".txt")) {

            for (QualityQuery t : topics) {

                System.out.printf("Searching for topic %s.%n", t.getQueryID());

                  //Perform the original query
                bq = new BooleanQuery.Builder();
                titleQuery = qp.multipleFieldsParse(t.getValue(TOPIC_FIELDS.TITLE));
                bq.add(titleQuery, BooleanClause.Occur.SHOULD);

                //Check the description field is not null/empty/blank
                String description = t.getValue(TOPIC_FIELDS.DESCRIPTION);
                if (description != null && !description.isEmpty() && !description.isBlank()) {
                    descriptionQuery = qp.multipleFieldsParse(description);
                    bq.add(descriptionQuery, BooleanClause.Occur.SHOULD);
                }

                //Execute the original query
                q = bq.build();
                docs = searcher.search(q, maxDocsRetrieved);
                sd = docs.scoreDocs;

                //Add the documents found to the result
                ArrayList<ScoreDoc> documents = new ArrayList<>(Arrays.asList(sd));

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

                //Sorting the retrieved documents by their score and cut the list to maxDocsRetrieved
                List<ScoreDoc> cutUniqueDocuments = null;
                if (!reSent && !reRead) {
                    documents.sort((o1, o2) -> Float.compare(o1.score, o2.score));
                    Collections.reverse(documents);
                    cutUniqueDocuments = documents.subList(0, maxDocsRetrieved);
                } else if (reSent && !reRead) {
                    sentimentOrder.sort((o1, o2) -> Float.compare(o1.score, o2.score));
                    Collections.reverse(sentimentOrder);
                    cutUniqueDocuments = sentimentOrder.subList(0, maxDocsRetrieved);
                } else {
                    readabilityOrder.sort((o1, o2) -> Float.compare(o1.score, o2.score));
                    Collections.reverse(readabilityOrder);
                    cutUniqueDocuments = readabilityOrder.subList(0, maxDocsRetrieved);
                }

                //print the results
                int i = 1;
                int pairsCounter = 1;
                //HasSet for removing duplicated document IDs in the search
                HashSet<String> docIDs = new HashSet<>();

                //HasSet for removing duplicated sentences pair in the search
                Set<String> stanceAndSentencesIDRetrieved = new HashSet<>();
                for (ScoreDoc document : cutUniqueDocuments) {

                    //retrieve the docID
                    docID = reader.document(document.doc, idField).get(ParsedDocument.FIELDS.ID);

                    //check if the docID was already retrieve
                    if (!docIDs.contains(docID)) {
                        docIDs.add(docID);
                        sentencesID = reader.document(document.doc, idField).getValues(ParsedDocument.FIELDS.SENTENCES);
                        stance = reader.document(document.doc, idField).get(ParsedDocument.FIELDS.STANCE);

                        //prepare premises and sentences id for the ToucheSentencesRetriever
                        Vector<String> premsIDs = new Vector<>();
                        Vector<String> conclIDs = new Vector<>();
                        for (String s : sentencesID) {
                            if (s.contains("PREM"))
                                premsIDs.add(s);
                            else if (s.contains("CONC"))
                                conclIDs.add(s);
                        }

                        //if the argument has no conclusion sentence id, add a custom one
                        if (conclIDs.isEmpty())
                            conclIDs.add(docID + "__CONC__1");

                        //retrieve the sentences pairs
                        ToucheSentencesRetriever sentRetr = new ToucheSentencesRetriever(premsIDs, conclIDs);

                        //write the sentences in the run output file (in the format required by CLEF)
                        for (String[] pair : sentRetr) {
                            if (pairsCounter == 1001)
                                break;
                            //create the sentences pair
                            String sentencesPair = String.format("%s\s%s\s%s,%s", t.getQueryID(), stance, pair[0], pair[1]);
                            //check if the sentences pair was already retrieved
                            if (!stanceAndSentencesIDRetrieved.contains(sentencesPair)) {
                                run.printf(Locale.ENGLISH, "%s\s%d\s%.2f\s%s%n", sentencesPair, pairsCounter++, document.score, runID);
                                stanceAndSentencesIDRetrieved.add(sentencesPair);
                            }
                        }

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
    public static final class TOPIC_FIELDS {

        /**
         * The title of a topic.
         */
        public static final String TITLE = "title";

        /**
         * The description of a topic.
         */
        public static final String DESCRIPTION = "description";

        /**
         * The narrative of a topic.
         */
        public static final String NARRATIVE = "narrative";
    }

}



