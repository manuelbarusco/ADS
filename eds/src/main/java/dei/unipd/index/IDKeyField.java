package index;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;

import java.io.Reader;

/**
 * Represents a {@link Field} for containing the ID of a sentence or a document or every field that contains key value such as acquisition time, stance and so on
 *
 * @author Manuel Barusco (manuel.barusco@studenti.unipd.it)
 * @version 1.00
 * @since 1.00
 */
public class IDKeyField extends Field {

    /**
     * The type of the document/sentence ID_FIELD
     */
    private static final FieldType IDKEY_TYPE = new FieldType();

    static {
        IDKEY_TYPE.setIndexOptions(IndexOptions.DOCS);
        IDKEY_TYPE.setStored(true);
    }

    /**
     * Create a new IDKeyField field
     *
     * @param field the name of the field that contains the ID or key value
     * @param value the content of the ID field
     */
    public IDKeyField(final String field, final Reader value) {
        super(field, value, IDKEY_TYPE);
    }

    /**
     * Create a new IDKeyField field
     *
     * @param field the name of the field that contains the ID
     * @param value the content of the ID field
     */
    public IDKeyField(final String field, final String value) {
        super(field, value, IDKEY_TYPE);
    }

}
