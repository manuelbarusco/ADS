package analyze;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.classic.ClassicTokenizer;
import org.apache.lucene.analysis.en.EnglishMinimalStemFilter;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static analyze.AnalyzerUtil.loadStopList;

/**
 * Analyzer for document processing, it performs tokenization and other processing techinques
 * this class was used only for parameter tuning tests. With this class we tested different stoplists and different stemmers
 *
 * @author Manuel Barusco (manuel.barusco@studenti.unipd.it)
 * @version 1.0
 * @since 1.0
 */
public class ToucheAnalyzerTuning extends Analyzer {

    private final String stopListFile;
    private final Class<TokenFilter> stemmer;

    /**
     * default constructor
     * it creates a new instance of the ToucheAnalyzer
     * @param stopList name of the stoplist to be used
     * @param stemmer class of the stem filter to be applied
     */
    public ToucheAnalyzerTuning(String stopList, Class<TokenFilter> stemmer) {
        super();
        this.stopListFile=stopList;
        this.stemmer=stemmer;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        final Tokenizer source = new ClassicTokenizer();
        //final Tokenizer source = new LetterTokenizer();
        TokenStream tokens = new LowerCaseFilter(source);
        tokens = new EnglishPossessiveFilter(tokens);
        tokens = new LengthFilter(tokens, 3, 20);

        if(stopListFile!=null && stemmer==null)
            tokens = new StopFilter(tokens, loadStopList(stopListFile));

        if(stopListFile == null && stemmer!=null) {
            //aplly only the stemmer
            try {
                Constructor<?> consStemmer = stemmer.getConstructor(TokenStreamComponents.class);
                Object stemFilter = consStemmer.newInstance(tokens);
                tokens = (TokenStream) stemFilter;
            } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }

        if(stopListFile != null && stemmer!=null) {
            //apply the stoplist
            tokens = new StopFilter(tokens, loadStopList(stopListFile));

            //aplly the stemmer
            try {
                Constructor<?> consStemmer = stemmer.getConstructor(TokenStreamComponents.class);
                Object stemFilter = consStemmer.newInstance(tokens);
                tokens = (TokenStream) stemFilter;
            } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }

        /*
        tokens = new EnglishMinimalStemFilter(tokens);
        tokens = new PorterStemFilter(tokens);
        tokens = new KStemFilter(tokens);
        tokens = new NGramTokenFilter(tokens, 3);
        tokens = new ShingleFilter(tokens, 2);*/

        return new Analyzer.TokenStreamComponents(source, tokens);
    }
}
