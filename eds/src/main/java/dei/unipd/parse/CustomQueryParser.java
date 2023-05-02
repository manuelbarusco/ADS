package dei.unipd.parse;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.search.Query;

import java.util.Map;

/**
 * Query parser
 * @author Manuel Barusco (manuel.barusco@studenti.unipd.it )
 * @version 1.00
 * @since 1.00
 */
public class CustomQueryParser {

    /**
     * The query parser used to create the query
     */
    private final QueryParser qp;

    /**
     * This map associates the weights to the dataset fields for the boosting
     */
    private final Map<String, Float> queryWeights;

    /**
     * The default field used to construct a {@code QueryParser}
     */
    private final String defaultField;

    /**
     * The analyzer used in the parsing
     */
    private final Analyzer analyzer;

    /**
     * Set up the custom query parser.
     *
     * @param queryWeights A map of field with weights
     * @param analyzer     The analyzer used to process the query
     * @param defaultField The default field used for single field parser
     */
    public CustomQueryParser(Map<String, Float> queryWeights, Analyzer analyzer, String defaultField) {

        if (queryWeights == null) {
            throw new NullPointerException("Hashmap of weights given as parameter cannot be null");
        }
        if (queryWeights.size() <= 0) {
            throw new NullPointerException("Hashmap of weights given as a parameter must contain at least 1 element");
        }
        if (analyzer == null) {
            throw new NullPointerException("Analyzer cannot be null");
        }
        if (defaultField == null) {
            throw new NullPointerException("Default field cannot be null");
        }
        if (defaultField.isEmpty()) {
            throw new NullPointerException("Default field cannot be empty");
        }
        if (!queryWeights.containsKey(defaultField)) {
            throw new NullPointerException("The hashmap given must contain an entry for the default field");
        }

        this.queryWeights = queryWeights;
        this.analyzer = analyzer;
        this.defaultField = defaultField;

        //Construct the default Lucene query parser
        qp = new QueryParser(defaultField, analyzer);

    }

    /**
     * Parse the string using a basic implementation of {@code QueryParserBase} provided by Lucene
     *
     * @param query The query to parse
     * @return a {@code Query} object
     * @throws ParseException if any error occurs while parsing the query given as parameter
     */
    public Query parse(String query) throws ParseException {
        String queryEscaped = QueryParserBase.escape(query);
        return qp.parse(queryEscaped);
    }

    /**
     * Parse multiple fields in the query using the implementation of {@code MultiFieldQueryParser} provided by the library
     * by considering the input query
     *
     * @param query The query to parse
     * @return a {@code Query} object
     * @throws ParseException if any error occurs while parsing the query given as parameter
     */
    public Query multipleFieldsParse(String query) throws ParseException{
        String queryEscaped = QueryParserBase.escape(query);

        String[] fields = new String[queryWeights.size()];
        queryWeights.keySet().toArray(fields);

        MultiFieldQueryParser mqp = new MultiFieldQueryParser(fields, analyzer, queryWeights);
        return mqp.parse(queryEscaped);
    }

}
