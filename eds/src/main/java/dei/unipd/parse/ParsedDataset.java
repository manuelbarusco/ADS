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

import org.apache.lucene.document.Field;

import java.util.Objects;

/**
 * Represents a parsed dataset to be indexed
 *
 * @author Manuel Barusco (manuel.barusco@studenti.unipd.it)
 * @version 1.00
 * @since 1.00
 */
public class ParsedDataset {

    /**
     * The names of the {@link Field}s within the index.
     * This is a constant static class of constants with the
     * dataset fields names
     *
     * @author Manuel Barusco (manuel.barusco@studenti.unipd.it)
     * @version 1.00
     * @since 1.00
     */
    public final static class FIELDS {

        /**
         * Field ID of the dataset
         */
        public static final String ID = "dataset_id";

        /**
         * Field TITLE of the dataset
         */
        public static final String TITLE = "title";

        /**
         * Field AUTHOR(s) of the document
         */
        public static final String AUTHOR = "author";

        /**
         * Field TAGS of the document
         */
        public static final String TAGS = "tags";

        /**
         * Field DESCRIPTION of the document
         */
        public static final String DESCRIPTION = "description";

        /**
         * Field ENTITIES of the document
         */
        public static final String ENTITIES = "entities";

        /**
         * Field CLASSES of the document
         */
        public static final String CLASSES = "classes";

        /**
         * Field LITERALS of the document
         */
        public static final String LITERALS = "literals";

        /**
         * Field PROPERTIES of the document
         */
        public static final String PROPERTIES = "properties";

        /**
         * Constant value for the empty dataset (cannot be indexed)
         */
        public static final int EMPTY = 0;

        /**
         * Constant value for the partial dataset (at least one file can be indexed)
         */
        public static final int PARTIAL = 1;

        /**
         * Constant value for the full dataset (all the files can be indexed)
         */
        public static final int FULL = 2;
    }


}
