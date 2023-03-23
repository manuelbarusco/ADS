package analyze;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.classic.ClassicTokenizer;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;

/**
 * Analyzer for the queries
 *
 * @author Manuel Barusco (manuel.barusco@studenti.unipd.it)
 */
public class ToucheAnalyzerQueries extends Analyzer {

    /**
     * Creates a new instance of the analyzer for process queries.
     */
    public ToucheAnalyzerQueries() {
        super();
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        final Tokenizer source = new ClassicTokenizer();
        //final Tokenizer source = new LetterTokenizer();

        TokenStream tokens = new LowerCaseFilter(source);
        tokens = new EnglishPossessiveFilter(tokens);
        tokens = new LengthFilter(tokens, 3, 20);

        /*tokens = new StopFilter(tokens, loadStopList("smart.txt"));
        tokens = new EnglishMinimalStemFilter(tokens);
        tokens = new PorterStemFilter(tokens);
        tokens = new KStemFilter(tokens);
        tokens = new NGramTokenFilter(tokens, 3);
        tokens = new ShingleFilter(tokens, 2);*/

        return new TokenStreamComponents(source, tokens);
    }

}