package search;

import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Vector;

/**
 * Object that retrieve from the searcher results the sentences pair
 *
 * @author Manuel Barusco (manuel.barusco@studenti.unipd.it)
 * @version 1.00
 * @since 1.00
 */

public class ToucheSentencesRetriever implements Iterable<String[]>, Iterator<String[]> {
    private final Vector<String> pid;
    private final Vector<String> cid;
    private final Iterator<String> itpid;
    private final Iterator<String> itcid;

    /**
     * Constructor
     *
     * @param premisedID   array of premises sentences
     * @param conclusionID array of conclusion sentences
     */
    public ToucheSentencesRetriever(Vector<String> premisedID, Vector<String> conclusionID) {
        if (premisedID == null || premisedID.isEmpty())
            throw new IllegalArgumentException("premisedID cannot be null or empty");
        if (conclusionID == null || conclusionID.isEmpty())
            throw new IllegalArgumentException("conclusionID cannot be null or empty");
        pid = premisedID;
        cid = conclusionID;
        itpid = pid.iterator();
        itcid = cid.iterator();
    }

    @NotNull
    @Override
    public Iterator<String[]> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return itpid.hasNext() && itcid.hasNext();
    }

    //this method returns a pair of sentences. String[0] is the first one in the pair and String[1] the second one
    @Override
    public String[] next() {
        if (!hasNext())
            throw new NoSuchElementException("No new pair to return");
        String[] pair = new String[2];
        pair[0] = itpid.next();
        pair[1] = itcid.next();
        return pair;
    }
}
