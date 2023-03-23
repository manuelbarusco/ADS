package analyze;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.tokenattributes.*;

import java.io.*;

/**
 * Helper class to load stop lists and <a href="http://opennlp.apache.org/" target="_blank">Apache OpenNLP</a> models
 * from the {@code resource} directory as well as consume {@link TokenStream}s and print diagnostic information about
 * them.
 *
 * @author Mario Giovanni Peloso (mariogiovanni.peloso@dei.unipd.it)
 * @version 1.0
 * @since 1.0
 */
class AnalyzerUtil {

    private static final ClassLoader CL = AnalyzerUtil.class.getClassLoader();

    /**
     * Consumes a {@link TokenStream} for the given text by using the provided {@link Analyzer} and prints diagnostic
     * information about all the generated tokens and their {@link org.apache.lucene.util.Attribute}s.
     *
     * @param a the analyzer to use.
     * @param t the text to process.
     * @throws IOException if something goes wrong while processing the text.
     */
    static void consumeTokenStream(final Analyzer a, final String t) throws IOException {

        // the start time of the processing
        final long start = System.currentTimeMillis();

        // Create a new TokenStream for a dummy field
        final TokenStream stream = a.tokenStream("field", new StringReader(t));

        // The term represented by the token

        // The type the token

        // Whether the token is a keyword. Keyword-aware TokenStreams/-Filters skip modification of tokens that are keywords

        // The position of the token wrt the previous token

        // The number of positions occupied by a token

        // The start and end offset of a token in characters

        // Optional flags a token can have


        try (stream) {
            final CharTermAttribute tokenTerm = stream.addAttribute(CharTermAttribute.class);
            final TypeAttribute tokenType = stream.addAttribute(TypeAttribute.class);
            final KeywordAttribute tokenKeyword = stream.addAttribute(KeywordAttribute.class);
            final PositionIncrementAttribute tokenPositionIncrement = stream.addAttribute(PositionIncrementAttribute.class);
            final PositionLengthAttribute tokenPositionLength = stream.addAttribute(PositionLengthAttribute.class);
            final OffsetAttribute tokenOffset = stream.addAttribute(OffsetAttribute.class);
            final FlagsAttribute tokenFlags = stream.addAttribute(FlagsAttribute.class);
            System.out.printf("####################################################################################%n");
            System.out.printf("Text to be processed%n");
            System.out.printf("+ %s%n%n", t);
            System.out.printf("Tokens%n");
            // Reset the stream before starting
            stream.reset();

            // Print all tokens until the stream is exhausted
            while (stream.incrementToken()) {
                System.out.printf("+ token: %s%n", tokenTerm.toString());
                System.out.printf("  - type: %s%n", tokenType.type());
                System.out.printf("  - keyword: %b%n", tokenKeyword.isKeyword());
                System.out.printf("  - position increment: %d%n", tokenPositionIncrement.getPositionIncrement());
                System.out.printf("  - position length: %d%n", tokenPositionLength.getPositionLength());
                System.out.printf("  - offset: [%d, %d]%n", tokenOffset.startOffset(), tokenOffset.endOffset());
                System.out.printf("  - flags: %d%n", tokenFlags.getFlags());
            }

            // Perform any end-of-stream operations
            stream.end();
        }

        // Close the stream and release all the resources

        System.out.printf("%nElapsed time%n");
        System.out.printf("+ %d milliseconds%n", System.currentTimeMillis() - start);
        System.out.printf("####################################################################################%n");
    }


    /**
     * Loads the required stop list among those available in the {@code resources} folder.
     *
     * @param stopFile the name of the file containing the stop list.
     * @return the stop list
     * @throws IllegalStateException if there is any issue while loading the stop list.
     */
    static CharArraySet loadStopList(final String stopFile) {

        if (stopFile == null) {
            throw new NullPointerException("Stop list file name cannot be null.");
        }

        if (stopFile.isEmpty()) {
            throw new IllegalArgumentException("Stop list file name cannot be empty.");
        }

        // the stop list
        CharArraySet stopList = null;

        try {

            // Get a reader for the file containing the stop list
            Reader in = new BufferedReader(new InputStreamReader(CL.getResourceAsStream("stoplists/"+stopFile)));

            // Read the stop list
            stopList = WordlistLoader.getWordSet(in);

            // Close the file
            in.close();

        } catch (IOException e) {
            throw new IllegalStateException(
                    String.format("Unable to load the stop list %s: %s", stopFile, e.getMessage()), e);
        }

        return stopList;
    }

}