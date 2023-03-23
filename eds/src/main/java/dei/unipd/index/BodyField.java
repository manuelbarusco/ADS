package index;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import parse.ParsedDocument;

import java.io.Reader;

/**
 * Represents a {@link Field} for containing the body of a document.
 * It's a tokenized field, not stored, keeping term frequencies, positions and offsets (see {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS})
 *
 * @author Mario Giovanni Peloso (ferro@dei.unipd.it)
 * @author Manuel Barusco (manuel.barusco@studenti.unipd.it)
 * @version 1.00
 * @since 1.00
 */
public class BodyField extends Field {

    /**
     * The type of the document body field
     */
    private static final FieldType BODY_TYPE = new FieldType();

    static {
        BODY_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        BODY_TYPE.setTokenized(true);
        BODY_TYPE.setStored(false);
        BODY_TYPE.setStoreTermVectors(true);
    }


    /**
     * Create a new field for the body of a document.
     *
     * @param value the contents of the body of a document.
     */
    public BodyField(final Reader value) {
        super(ParsedDocument.FIELDS.SOURCE_TEXT, value, BODY_TYPE);
    }

    /**
     * Create a new field for the body of a document.
     *
     * @param value the contents of the body of a document.
     */
    public BodyField(final String value) {
        super(ParsedDocument.FIELDS.SOURCE_TEXT, value, BODY_TYPE);
    }

}
