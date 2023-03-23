package search;

import com.vader.sentiment.analyzer.SentimentAnalyzer;
import io.whelk.flesch.kincaid.ReadabilityCalculator;
import org.apache.commons.lang3.Range;
import org.apache.lucene.benchmark.quality.QualityQuery;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.ScoreDoc;
import parse.ParsedDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Re-score the documents using sentiment analysis
 *
 * @author Elham Soleymani (elham.soleymani@studenti.unipd.it)
 * @author Riccardo Forzan (riccardo.forzan@studenti.unipd.it)
 * @version 1.00
 * @since 1.00
 */
public class Ranker {

    private final IndexReader reader;
    private final QualityQuery query;
    private final ArrayList<ScoreDoc> documents;

    private static final float FIRST_INTERVAL = 34.1f;;
    private static final float SECOND_INTERVAL = 47.7f;;
    private static final float THIRD_INTERVAL = 49.8f;

    /**
     * Constructs a ranker given a query and a list of documents
     *
     * @param reader index reader object
     * @param query that generated the documents
     * @param documents retrieved by the query
     */
    public Ranker(IndexReader reader, QualityQuery query, ArrayList<ScoreDoc> documents) {
        this.reader = reader;
        this.query = query;
        this.documents = documents;
    }

    /**
     * Assign sentiment score to a document
     *
     * @param text string that contains the text of the document
     * @return score sentiment score of the document
     */
    public static float getDocumentSentimentScore(String text){
        SentimentAnalyzer textSentimentAnalyzer;
        float sentimentScore = 0;
        try {
            textSentimentAnalyzer = new SentimentAnalyzer(text);
            textSentimentAnalyzer.analyze();
            sentimentScore = textSentimentAnalyzer.getPolarity().get("compound");
        } catch (IOException e) {
            //If some error occurs then do not modify the list
            System.out.println("Re-ranking aborted, keeping the one given by Lucene : " + e.getMessage());
        }
        return sentimentScore;
    }

    /**
     * assign readability score to a document
     * @param text string that contains the text of the document
     * @return score readability score of the document
     */
    public static float getDocumentReadabilityScore(String text){
       return (float) ReadabilityCalculator.calculateReadingEase(text);
    }

    /**
     * Returns the same list of documents that has been used to construct this object, but the scores associated to
     * the documents are recomputed. Using sentiment analysis re ranks the document by placing before documents
     * which score is closer to the score obtained by the query
     *
     * @return reordered list of documents
     */
    public List<ScoreDoc> rankUsingSentiment(){

        ArrayList<ScoreDoc> documentsClone = (ArrayList<ScoreDoc>) documents.clone();

        SentimentAnalyzer querySentimentAnalyzer;
        float querySentimentScore;
        try {
            querySentimentAnalyzer = new SentimentAnalyzer(query.getValue("title"));
            querySentimentAnalyzer.analyze();
            querySentimentScore = querySentimentAnalyzer.getPolarity().get("compound");
        } catch (IOException e) {
            //If some error occurs then do not modify the list
            System.out.println("Re-ranking aborted, keeping the one given by Lucene : " + e.getMessage());
            return documents;
        }

        float upperLimit;
        float lowerLimit;

        upperLimit = (float) (querySentimentScore + (querySentimentScore * FIRST_INTERVAL));
        lowerLimit = (float) (querySentimentScore - (querySentimentScore * FIRST_INTERVAL));
        Range<Float> firstSigma = Range.between(upperLimit,lowerLimit);

        upperLimit = (float) (querySentimentScore + (querySentimentScore * SECOND_INTERVAL));
        lowerLimit = (float) (querySentimentScore - (querySentimentScore * SECOND_INTERVAL));
        Range<Float> secondSigma = Range.between(upperLimit,lowerLimit);

        upperLimit = (float) (querySentimentScore + (querySentimentScore * THIRD_INTERVAL));
        lowerLimit = (float) (querySentimentScore - (querySentimentScore * THIRD_INTERVAL));
        Range<Float> thirdSigma = Range.between(upperLimit,lowerLimit);

        Document indexDocument;
        SentimentAnalyzer documentSentimentAnalyzer;
        float documentSentimentScore;
        float newScore;

        try {
            for (ScoreDoc doc : documentsClone) {
                indexDocument = reader.document(doc.doc);
                //System.out.printf("conclusion: " + indexDocument.get(ParsedDocument.FIELDS.CONCLUSION));
                if(indexDocument.get(ParsedDocument.FIELDS.CONCLUSION) == null)
                    documentSentimentAnalyzer = new SentimentAnalyzer("");
                else
                    documentSentimentAnalyzer = new SentimentAnalyzer(indexDocument.get(ParsedDocument.FIELDS.CONCLUSION));
                documentSentimentAnalyzer.analyze();
                documentSentimentScore = documentSentimentAnalyzer.getPolarity().get("compound");
                System.out.println(documentSentimentScore);
                if (firstSigma.contains(documentSentimentScore)) {
                    newScore = doc.score + doc.score * documentSentimentScore;
                    doc.score = newScore;
                } else if (secondSigma.contains(documentSentimentScore)) {
                    newScore = doc.score + doc.score * 0.5f * documentSentimentScore;
                    doc.score = newScore;
                } else if (thirdSigma.contains(documentSentimentScore)) {
                    newScore = doc.score + doc.score * 0.10f * documentSentimentScore;
                    doc.score = newScore;
                }
            }
        } catch (IOException e){
            System.out.println("An error occurred while re-ranking documents: " + e.getMessage());
        }

        //Reorder the documents based on their new scores
        documentsClone.sort(Comparator.comparingDouble(o -> o.score));
        Collections.reverse(documentsClone);

        return documentsClone;

    }

    /**
     * Returns the same list of documents that has been used to construct this object, but the scores associated to
     * the documents are recomputed. Using Flesch-Kincaid readability metrics, re ranks the document collection
     * by placing before documents which score is higher
     * @see <a href="https://en.wikipedia.org/wiki/Flesch%E2%80%93Kincaid_readability_tests">Flesch–Kincaid readability tests</a>
     *
     *
     * @return reordered list of documents
     */
    public List<ScoreDoc> rankByReadability(){

        ArrayList<ScoreDoc> documentsClone = (ArrayList<ScoreDoc>) documents.clone();
        float newScore;

        for(ScoreDoc document:documentsClone) {
            try {
                Document indexDocument = reader.document(document.doc);
                String field = indexDocument.get(ParsedDocument.FIELDS.CONCLUSION);
                //Calculate readability on the conclusion field
                float readabilityScore = (float) ReadabilityCalculator.calculateReadingEase(field);
                //Calculate the new score
                newScore = document.score + document.score * readabilityScore;
                document.score = newScore;
            } catch (IOException e) {
                System.out.println("An error occurred while re-ranking documents: " + e.getMessage());
            }
        }

        //Reorder the documents based on their new scores
        documentsClone.sort(Comparator.comparingDouble(o -> o.score));
        Collections.reverse(documentsClone);

        return documentsClone;

    }

}
