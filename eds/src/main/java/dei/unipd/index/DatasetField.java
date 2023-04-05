package dei.unipd.index;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;

import java.io.Reader;

/**
 * Represents a {@link Field} for containing the content of a meta or content field
 * of the dataset
 *
 * @author Manuel Barusco (manuel.barusco@studenti.unipd.it)
 * @version 1.00
 * @since 1.00
 */
public class DatasetField extends Field {

    /**
     * The type of the document body field
     */
    private static final FieldType DATASET_TYPE = new FieldType();

    //defining the fields options imposed by ACORDAR baseline
    static {
        DATASET_TYPE.setStored(true);
        DATASET_TYPE.setTokenized(true);
        DATASET_TYPE.setStoreTermVectors(true);
        DATASET_TYPE.setStoreTermVectorPositions(true);
        DATASET_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    }

    /**
     * Create a new Dataset Field
     *
     * @param field the name of the field
     * @param value the content of the field
     */
    public DatasetField(final String field, final Reader value) {
        super(field, value, DATASET_TYPE);
    }

    /**
     * Create a new Dataset Field
     *
     * @param field the name of the field
     * @param value the content of the field
     */
    public DatasetField(final String field, final String value) {
        super(field, value, DATASET_TYPE);
    }

}
