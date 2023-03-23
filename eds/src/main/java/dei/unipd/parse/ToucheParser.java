/*
 *  Copyright 2017-2022 University of Padua, Italy
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
package parse;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import utils.Constants;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * CSV + JSON Parser using Jackson Library and Apache CSV Parser Library
 *
 * @author Manuel Barusco (manuel.barusco@studenti.unipd.it)
 * @author Riccardo Forzan (riccardo.forzan@studenti.unipd.it)
 * @version 1.00
 * @since 1.00
 */
public class ToucheParser extends DocumentParser {

    /**
     * Headers specified in the CSV class
     */
    public enum Headers {
        /**
         * id header field of the CSV file
         */
        id,
        /**
         * conclusion header field of the CSV file
         */
        conclusion,
        /**
         * premises header field of the CSV file
         */
        premises,
        /**
         * context header field of the CSV file
         */
        context,
        /**
         * sentences header field of the CSV file
         */
        sentences
    }

    /**
     * The currently parsed document
     */
    private ParsedDocument document;

    /**
     * CSV records read from the csv file
     */
    private Iterable<CSVRecord> records;

    /**
     * Iterator for the CSV record
     */
    private Iterator<CSVRecord> iterator;

    /**
     * Creates a new document parser.
     *
     * @param in the reader to the document(s) to be parsed.
     * @throws NullPointerException     if {@code in} is {@code null}
     * @throws IllegalArgumentException if any error occurs while creating the parse
     * @throws IOException              if any error occurs while parsing the document
     */
    public ToucheParser(final Reader in) throws IOException {
        super(new BufferedReader(in));
        //Build custom CSV reader
        CSVFormat.Builder builder = CSVFormat.Builder.create();
        //RFC4180 rules
        builder.setDelimiter(',');
        builder.setQuote('"');
        builder.setRecordSeparator("\r\n");
        builder.setIgnoreEmptyLines(false);
        //Add header for parsing
        builder.setHeader(ToucheParser.Headers.class);
        //build the reader
        records = builder.build().parse(in);
        iterator = records.iterator();
        //Skip the header line of the CSV input file
        iterator.next();
    }

    @Override
    public boolean hasNext() {

        //Json Parser needed for parsing csv json fields
        JsonParser jparser;
        if (!iterator.hasNext())
            return false;

        CSVRecord record = iterator.next();

        //CSV fields data
        String id = record.get(ToucheParser.Headers.id);
        String conclusion = record.get(ToucheParser.Headers.conclusion);

        //Remove non-printable characters from input CSV fields
        Pattern nonPrintableFilter = Pattern.compile("\\\\x\\p{XDigit}{2}");
        String premises = nonPrintableFilter.matcher(record.get(Headers.premises)).replaceAll("");
        String context = nonPrintableFilter.matcher(record.get(Headers.context)).replaceAll("");
        String sentences = nonPrintableFilter.matcher(record.get(Headers.sentences)).replaceAll("");

        //Catch exceptions while parsing
        try {

            //JSON fields needed for the document construction
            String stance;
            String acquisitionTime;
            String discussionTitle;
            String url;
            String sourceTitle;
            String sourceText;

            //parsing premises json field
            jparser = new JsonFactory().enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES)
                    //TODO: remove usage of deprecated method
                    .enable(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER)
                    .createParser(premises);
            while (true) {
                if (jparser.nextToken().equals(JsonToken.START_ARRAY))
                    break;
            }
            JsonToken token = jparser.nextToken();

            if (!token.equals(JsonToken.START_OBJECT)) {
                throw new IllegalStateException();
            }

            JsonNode root = new ObjectMapper().readTree(jparser);
            stance = root.has("stance") ? root.get("stance").toString().replace("\"", "") : null;

            //parsing context json field
            jparser = new JsonFactory()
                    .enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES)
                    //TODO: remove usage of deprecated method
                    .enable(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER)
                    .createParser(context);
            if (!jparser.nextToken().equals(JsonToken.START_OBJECT)) {
                throw new IllegalStateException();
            }

            root = new ObjectMapper().readTree(jparser);

            acquisitionTime = root.has("acquisitionTime") ? root.get("acquisitionTime").toString() : null;
            discussionTitle = root.has("discussionTitle") ? root.get("discussionTitle").toString() : null;
            url = root.has("sourceUrl") ? root.get("sourceUrl").toString() : null;
            sourceTitle = root.has("sourceTitle") ? root.get("sourceTitle").toString() : null;
            sourceText = root.has("sourceText") ? root.get("sourceText").toString() : null;

            //parsing CSV sentences field
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
            mapper.enable(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER);
            ParsedDocument.Sentence[] sentencesObj = mapper.readValue(sentences, ParsedDocument.Sentence[].class);

            /*ONLY FOR DEBUG PURPOSE
            System.out.println(acquisitionTime);
            System.out.println(discussionTitle);
            System.out.println(sourceDomain);
            System.out.println(url);
            System.out.println(sourceTitle);
            System.out.println(sourceText);*/

            document = new ParsedDocument(id, conclusion, stance, acquisitionTime, discussionTitle, url, sourceTitle, sourceText, sentencesObj);

        } catch (Throwable e) {
            System.out.println(e);

            //ONLY FOR DEBUG PURPOSE
            //System.out.println(record);
        }

        return true;

    }

    @Override
    protected final ParsedDocument parse() {
        return document;
    }

}
