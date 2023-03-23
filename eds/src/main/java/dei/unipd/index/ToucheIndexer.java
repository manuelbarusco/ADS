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

package index;

import analyze.ToucheAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;
import parse.DocumentParser;
import parse.ParsedDocument;
import parse.ToucheParser;
import utils.Constants;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Indexer object for indexing the Touche Collection
 *
 * @author Manuel Barusco (manuel.barusco@studenti.unipd.it)
 * @author Mario Giovanni Peloso (mariogiovanni.peloso@studenti.unipd.it)
 * @version 1.00
 * @since 1.00
 */
public class ToucheIndexer {

    /**
     * One megabyte constant
     */
    private static final int MBYTE = 1024 * 1024;

    /**
     * The index writer.
     */
    private final IndexWriter writer;

    /**
     * The class of the {@code DocumentParser} to be used for collection parsing
     */
    private final Class<? extends ToucheParser> dpCls;

    /**
     * The directory (and eventually sub-directories) where documents are stored.
     */
    private final Path docsDir;

    /**
     * The extension of the files to be indexed.
     */
    private final String extension;

    /**
     * The charset used for encoding documents.
     */
    private final Charset cs;

    /**
     * The total number of documents expected to be indexed.
     */
    private final long expectedDocs;

    /**
     * The start instant of the indexing.
     */
    private final long start;

    /**
     * The total number of indexed files.
     */
    private long filesCount;

    /**
     * The total number of indexed documents.
     */
    private long docsCount;

    /**
     * The total number of indexed bytes
     */
    private long bytesCount;

    /**
     * Creates a new indexer
     *
     * @param analyzer        the {@code Analyzer} to be used.
     * @param similarity      the {@code Similarity} to be used.
     * @param ramBufferSizeMB the size in megabytes of the RAM buffer for indexing documents.
     * @param indexPath       the directory where to store the index.
     * @param docsPath        the directory from which documents have to be read.
     * @param extension       the extension of the files to be indexed.
     * @param charsetName     the name of the charset used for encoding documents.
     * @param expectedDocs    the total number of documents expected to be indexed
     * @param dpCls           the class of the {@code DocumentParser} to be used.
     * @throws NullPointerException     if any of the parameters is {@code null}.
     * @throws IllegalArgumentException if any of the parameters assumes invalid values.
     */
    public ToucheIndexer(final Analyzer analyzer, final Similarity similarity, final int ramBufferSizeMB,
                         final String indexPath, final String docsPath, final String extension,
                         final String charsetName, final long expectedDocs,
                         final Class<? extends ToucheParser> dpCls) {

        if (dpCls == null) {
            throw new NullPointerException("Document parser class cannot be null.");
        }

        this.dpCls = dpCls;

        if (analyzer == null) {
            throw new NullPointerException("Analyzer cannot be null.");
        }

        if (similarity == null) {
            throw new NullPointerException("Similarity cannot be null.");
        }

        if (ramBufferSizeMB <= 0) {
            throw new IllegalArgumentException("RAM buffer size cannot be less than or equal to zero.");
        }

        //setting up the Lucene IndexWriter object
        final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setSimilarity(similarity);
        iwc.setRAMBufferSizeMB(ramBufferSizeMB);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setCommitOnClose(true);
        iwc.setUseCompoundFile(true);

        if (indexPath == null) {
            throw new NullPointerException("Index path cannot be null.");
        }

        if (indexPath.isEmpty()) {
            throw new IllegalArgumentException("Index path cannot be empty.");
        }

        final Path indexDir = Paths.get(indexPath);

        // if the directory does not already exist, create it
        if (Files.notExists(indexDir)) {
            try {
                Files.createDirectory(indexDir);
            } catch (Exception e) {
                throw new IllegalArgumentException(String.format("Unable to create directory %s: %s.", indexDir.toAbsolutePath(), e.getMessage()), e);
            }
        }

        if (!Files.isWritable(indexDir)) {
            throw new IllegalArgumentException(String.format("Index directory %s cannot be written.", indexDir.toAbsolutePath()));
        }

        if (!Files.isDirectory(indexDir)) {
            throw new IllegalArgumentException(String.format("%s expected to be a directory where to write the index.", indexDir.toAbsolutePath()));
        }

        if (docsPath == null) {
            throw new NullPointerException("Documents path cannot be null.");
        }

        if (docsPath.isEmpty()) {
            throw new IllegalArgumentException("Documents path cannot be empty.");
        }

        final Path docsDir = Paths.get(docsPath);
        if (!Files.isReadable(docsDir)) {
            throw new IllegalArgumentException(
                    String.format("Documents directory %s cannot be read.", docsDir.toAbsolutePath().toString()));
        }

        if (!Files.isDirectory(docsDir)) {
            throw new IllegalArgumentException(
                    String.format("%s expected to be a directory of documents.", docsDir.toAbsolutePath().toString()));
        }

        this.docsDir = docsDir;

        if (extension == null) {
            throw new NullPointerException("File extension cannot be null.");
        }

        if (extension.isEmpty()) {
            throw new IllegalArgumentException("File extension cannot be empty.");
        }
        this.extension = extension;

        if (charsetName == null) {
            throw new NullPointerException("Charset name cannot be null.");
        }

        if (charsetName.isEmpty()) {
            throw new IllegalArgumentException("Charset name cannot be empty.");
        }

        try {
            cs = Charset.forName(charsetName);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format("Unable to create the charset %s: %s.", charsetName, e.getMessage()), e);
        }

        if (expectedDocs <= 0) {
            throw new IllegalArgumentException(
                    "The expected number of documents to be indexed cannot be less than or equal to zero.");
        }
        this.expectedDocs = expectedDocs;

        this.docsCount = 0;

        this.bytesCount = 0;

        this.filesCount = 0;

        try {
            writer = new IndexWriter(FSDirectory.open(indexDir), iwc);
        } catch (IOException e) {
            throw new IllegalArgumentException(String.format("Unable to create the index writer in directory %s: %s.",
                    indexDir.toAbsolutePath().toString(), e.getMessage()), e);
        }

        this.start = System.currentTimeMillis();

    }

    /**
     * Indexes the documents.
     *
     * @throws IOException if something goes wrong while indexing.
     */
    public void index() throws IOException {

        System.out.printf("%n#### Start indexing ####%n");

        //visit the collection directory
        Files.walkFileTree(docsDir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                //file extension check
                if (file.getFileName().toString().endsWith(extension)) {

                    //creating the ToucheParser
                    DocumentParser dp = DocumentParser.create(dpCls, Files.newBufferedReader(file, cs));

                    bytesCount += Files.size(file);

                    filesCount += 1;

                    Document doc = null; //Lucene Document

                    for (ParsedDocument pd : dp) {

                        //create the Lucene Document
                        doc = new Document();

                        //add the document identifier
                        doc.add(new StringField(ParsedDocument.FIELDS.ID, pd.getIdentifier(), Field.Store.YES));

                        //add the document text
                        doc.add(new BodyField(pd.getSourceText()));

                        //add additional body correlated information (Conclusion)
                        doc.add(new BodyCorrelatedField(ParsedDocument.FIELDS.CONCLUSION, pd.getConclusion()));

                        //add additional body correlated information (Discussion Title)
                        doc.add(new BodyCorrelatedField(ParsedDocument.FIELDS.DISCUSSION_TITLE, pd.getDiscussionTitle()));

                        //add additional body correlated information (Source Title)
                        doc.add(new BodyCorrelatedField(ParsedDocument.FIELDS.SOURCE_TITLE, pd.getSourceTitle()));

                        //add document Stance
                        doc.add(new IDKeyField(ParsedDocument.FIELDS.STANCE, pd.getStance()));

                        //add document sentences id
                        ParsedDocument.Sentence[] sentences = pd.getSentences();
                        for (ParsedDocument.Sentence s : sentences) {
                            doc.add(new StringField(ParsedDocument.FIELDS.SENTENCES, s.getID(), Field.Store.YES));
                        }

                        writer.addDocument(doc); //index the document

                        docsCount++;

                        // print progress every 10000 indexed documents, only for debug purpose
                        if (docsCount % 10000 == 0) {
                            System.out.printf("%d document(s) (%d files, %d Mbytes) indexed in %d seconds.%n",
                                    docsCount, filesCount, bytesCount / MBYTE,
                                    (System.currentTimeMillis() - start) / 1000);
                        }

                    }

                }
                return FileVisitResult.CONTINUE;
            }
        });

        //indexer commit and resource release
        writer.commit();
        writer.close();

        if (docsCount != expectedDocs) {
            System.out.printf("Expected to index %d documents; %d indexed instead.%n", expectedDocs, docsCount);
        }

        System.out.printf("%d document(s) (%d files, %d Mbytes) indexed in %d seconds.%n", docsCount, filesCount,
                bytesCount / MBYTE, (System.currentTimeMillis() - start) / 1000);

        System.out.printf("#### Indexing complete ####%n");
    }

    /**
     * ONLY FOR DEBUGGING PURPOSE
     *
     * @param args command line arguments.
     * @throws Exception if something goes wrong while indexing.
     */
    public static void main(String[] args) throws Exception {

        final int ramBuffer = 256;
        final String indexPath = Constants.manuelIndexPath;
        final String docsPath = Constants.manuelDocsPath;

        final String extension = "csv";
        final int expectedDocs = 365408;
        final String charsetName = "ISO-8859-1";

        final Analyzer a = new ToucheAnalyzer();

        final Similarity sim = new LMDirichletSimilarity(1800);

        ToucheIndexer i = new ToucheIndexer(a, sim, ramBuffer, indexPath, docsPath, extension,
                charsetName, expectedDocs, ToucheParser.class);

        i.index();

    }

}
