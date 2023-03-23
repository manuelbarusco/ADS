package index;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;

import java.io.Reader;

/**
 * Represents a {@link Field} for containing additional information for the body field such as conclusion, stance, discussion title, source title and so on
 * It's a tokenized field, not stored, keeping term frequencies, positions and offsets (see {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS})
 *
 * @author Manuel Barusco (manuel.barusco@studenti.unipd.it)
 * @version 1.00
 * @since 1.00
 */
public class BodyCorrelatedField extends Field {

    /**
     * The type of the document body field
     */
    private static final FieldType BODYCORRELATED_TYPE = new FieldType();

    static {
        BODYCORRELATED_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        BODYCORRELATED_TYPE.setTokenized(true);
        BODYCORRELATED_TYPE.setStored(false);
        BODYCORRELATED_TYPE.setStoreTermVectors(true);
    }

    /**
     * Create a new field for additional body-correlated information
     *
     * @param field the name of the field that contains additional body-correlated information
     * @param value the content of the field
     */
    public BodyCorrelatedField(final String field, final Reader value) {
        super(field, value, BODYCORRELATED_TYPE);
    }

    /**
     * Create a new field for additional body-correlated information
     *
     * @param field the name of the field that contains additional body-correlated information
     * @param value the content of the field
     */
    public BodyCorrelatedField(final String field, final String value) {
        super(field, value, BODYCORRELATED_TYPE);
    }

}
