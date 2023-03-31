/*
 *  Copyright 2021-2022 University of Padua, Italy
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dei.unipd.parse;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.lucene.document.Field;

import java.util.Objects;

/**
 * Represents a parsed dataset to be indexed
 *
 * @author Manuel Barusco (manuel.barusco@studenti.unipd.it)
 * @version 1.00
 * @since 1.00
 */
public class ParsedDocument {

    /**
     * The id of the dataset extracted from the json file
     */
    private final String id;

    /**
     * The title of the dataset extracted from the json file
     */
    private final String sourceTitle;

    /**
     * The authors of the dataset extracted from the json file
     */
    private final String authors;

    /**
     * The tags of the dataset extracted from the json file
     */
    private final String tags;

    /**
     * The subjects of the dataset extracted from the datasets files
     */
    private String subjects


    /**
     *
     * @param id the unique document identifier
     * @param conclusion conclusions of the document
     * @param stance stance of the document
     * @param acquisitionTime acquisition time of the document
     * @param discussionTitle discussion title of the document
     * @param url url of the document
     * @param sourceTitle source title of the document
     * @param sourceText source text of the document
     * @param sentences sentences of the document
     * @throws IllegalArgumentException if ID is null, empty or blank
     * @throws IllegalArgumentException if sourceText is null, empty or blank
     * @throws IllegalArgumentException if sentences is null or empty
     */

    public ParsedDocument(final String id, final String conclusion, final String stance, final String acquisitionTime, final String discussionTitle, final String url, final String sourceTitle, final String sourceText, final Sentence[] sentences) {

        //check the main arguments
        if (id == null || id.isEmpty() || id.isBlank()) {
            throw new IllegalArgumentException("ID field cannot be null, empty or only white spaces");
        }

        if (sourceText == null || sourceText.isEmpty() || sourceText.isBlank()) {
            throw new IllegalArgumentException("Source text field cannot be null, empty or only white spaces");
        }

        if (sentences == null || sentences.length == 0) {
            throw new IllegalArgumentException("Sentence field cannot be null, empty or only white spaces");
        }

        this.discussionTitle = Objects.requireNonNullElse(discussionTitle, "");

        //initialization of the object
        this.id = id;
        this.conclusion = conclusion;
        this.stance = stance;
        this.acquisitionTime = acquisitionTime;
        this.url = url;
        this.sourceTitle = sourceTitle;
        this.sourceText = sourceText;
        this.sentences = sentences;
    }

    /**
     * Returns the document ID
     *
     * @return the unique document identifier.
     */
    public String getIdentifier() {
        return id;
    }

    /**
     * Returns the conclusion of the discussion/document
     *
     * @return the conclusion of the discussion/document
     */
    public String getConclusion() {
        return conclusion;
    }

    /**
     * Returns the stance of the document (PRO or CON)
     *
     * @return the stance of the document (PRO or CON)
     */
    public String getStance() {
        return stance;
    }

    /**
     * Returns the datetime when the document was acquired
     *
     * @return the datetime when the document was acquired
     */
    public String getAcquisitionTime() {
        return acquisitionTime;
    }

    /**
     * Returns the title of the document/discussion
     *
     * @return the title of the document/discussion
     */
    public String getDiscussionTitle() {
        return discussionTitle;
    }

    /**
     * Returns the source url of the document
     *
     * @return the source url of the document
     */
    public String getUrl() {
        return url;
    }

    /**
     * Returns the title of the document
     *
     * @return the title of the document
     */
    public String getSourceTitle() {
        return sourceText;
    }

    /**
     * Returns the text of the document
     *
     * @return the text of the document
     */
    public String getSourceText() {
        return sourceText;
    }

    /**
     * Returns the sentences in which the document is split
     *
     * @return the sentences in which the document is split
     */
    public Sentence[] getSentences() {
        return sentences;
    }

    @Override
    public final String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE)
                .append(FIELDS.ID, id)
                .append(FIELDS.CONCLUSION, conclusion)
                .append(FIELDS.STANCE, stance)
                .append(FIELDS.ACQUISITION_TIME, acquisitionTime)
                .append(FIELDS.DISCUSSION_TITLE, discussionTitle)
                .append(FIELDS.URL, url)
                .append(FIELDS.SOURCE_TITLE, sourceTitle)
                .append(FIELDS.SOURCE_TEXT, sourceText)
                .append(FIELDS.SENTENCES, sentences);

        return tsb.toString();
    }

    @Override
    public final boolean equals(Object o) {
        return (this == o) || ((o instanceof ParsedDocument) && id.equals(((ParsedDocument) o).id));
    }

    @Override
    public final int hashCode() {
        return 37 * id.hashCode();
    }

    /**
     * Represents a sentence in the collection that have to be indexed,
     * is also a mapper class for Jackson
     *
     * @author Manuel Barusco (manuel.barusco@studenti.unipd.it)
     * @version 1.00
     * @since 1.00
     */
    public static class Sentence {

        //ID of the sentence
        private String sent_id;

        //Text of the sentence
        private String sent_text;

        /**
         * default constructor
         */
        public Sentence() {
            sent_id = "";
            sent_text = "";
        }

        /**
         * @param id id of the sentencee
         * @throws IllegalArgumentException if id is null, empty or blank
         */
        public void setSent_id(String id) {
            if (id == null || id.isEmpty() || id.isBlank())
                throw new IllegalArgumentException("Sent_id cannot be null");
            sent_id = id;
        }

        /**
         * @param text text of the sentencee
         * @throws IllegalArgumentException if texts is null, empty or blank
         */
        public void setSent_text(String text) {
            if (text == null || text.isEmpty() || text.isBlank())
                throw new IllegalArgumentException("Sent_text cannot be null");
            sent_text = text;
        }

        /**
         * @return sent_id of the sentence
         */
        public String getID() {
            return sent_id;
        }

        /**
         * @return sent_text of the sentence
         */
        public String getText() {
            return sent_text;
        }

        /**
         * @return String representation of the sentence
         */
        public String toString() {
            return "Sent_ID: " + sent_id + ", Sent_Text: " + sent_text;
        }
    }

    /**
     * The names of the {@link Field}s within the index.
     *
     * @author Manuel Barusco (manuel.barusco@studenti.unipd.it)
     * @version 1.00
     * @since 1.00
     */
    public final static class FIELDS {

        /**
         * Field ID of the document
         */
        public static final String ID = "id";

        /**
         * Field CONCLUSION of the document
         */
        public static final String CONCLUSION = "conclusion";

        /**
         * Field STANCE of the document
         */
        public static final String STANCE = "premisesStance";

        /**
         * Field ACQUISITION_TIME of the document
         */
        public static final String ACQUISITION_TIME = "acquisitionTime";

        /**
         * Field DISCUSSION_TITLE of the document
         */
        public static final String DISCUSSION_TITLE = "discussionTitle";

        /**
         * Field URL of the document
         */
        public static final String URL = "url";

        /**
         * Field SOURCE_TITLE of the document
         */
        public static final String SOURCE_TITLE = "sourceTitle";

        /**
         * Field SOURCE_TEXT of the document
         */
        public static final String SOURCE_TEXT = "sourceText";

        /**
         * Field SENTENCES of the document
         */
        public static final String SENTENCES = "sentences";
    }


}
