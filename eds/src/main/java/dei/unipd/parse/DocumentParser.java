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


package parse;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Represents a document parser.
 *
 * @author Manuel Barusco (manuel.barusco@studenti.unipd.it)
 * @version 1.00
 * @since 1.00
 */
public abstract class DocumentParser implements Iterator<ParsedDocument>, Iterable<ParsedDocument> {

    /**
     * Indicates whether there is another {@code ParsedDocument} to return.
     */
    protected final boolean hasNext = true;

    /**
     * The reader to be used to parse document(s).
     */
    protected final Reader in;

    /**
     * Creates a new document parser.
     *
     * @param in the reader to the document(s) to be parsed.
     * @throws NullPointerException if {@code in} is {@code null}.
     */
    protected DocumentParser(final Reader in) {

        if (in == null) {
            throw new NullPointerException("Reader cannot be null.");
        }

        this.in = in;
    }

    /**
     * Creates a new {@code DocumentParser}.
     * <p>
     * It assumes the {@code DocumentParser} has a single-parameter constructor which takes a {@code Reader} as input.
     *
     * @param cls the class of the document parser to be instantiated.
     * @param in  the reader to the document(s) to be parsed.
     * @return a new instance of {@code DocumentParser} for the given class.
     * @throws NullPointerException  if {@code cls} and/or {@code in} are {@code null}.
     * @throws IllegalStateException if something goes wrong in instantiating the class.
     */
    public static DocumentParser create(Class<? extends DocumentParser> cls, Reader in) {

        if (cls == null) {
            throw new NullPointerException("Document parser class cannot be null.");
        }

        if (in == null) {
            throw new NullPointerException("Reader cannot be null.");
        }

        try {
            return cls.getConstructor(Reader.class).newInstance(in);
        } catch (Exception e) {
            throw new IllegalStateException(String.format("Unable to instantiate document parser %s.", cls.getName()), e);
        }

    }

    @Override
    public final Iterator<ParsedDocument> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public final ParsedDocument next() {

        if (!hasNext) {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            throw new NoSuchElementException("No more documents to parse.");
        }

        return parse();
    }

    /**
     * Performs the actual parsing of the document.
     *
     * @return the parsed document.
     */
    protected abstract ParsedDocument parse();

}
