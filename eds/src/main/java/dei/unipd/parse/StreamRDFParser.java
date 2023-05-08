package dei.unipd.parse;

import org.apache.jena.atlas.iterator.IteratorCloseable;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.AsyncParser;

import java.io.File;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * @author Manuel Barusco
 * This class use Apache Jena in order to read and extract info from RDF files
 * such as .ttl and .rdf files. The objects of this class will parse all the RDF files in a stream fashion
 * in order to save memory and deal with big files.
 * WARNING: if a file has a syntax error the parser will stop and it will throw the exception that Jena is throwing
 * NB: This class is implemented as an iterator object
 */
public class StreamRDFParser implements Iterator<StreamRDFParser.CustomTriple> {

    private String path;                        //path to the file to be parsed

    private IteratorCloseable<Triple> iterator; //stream-based iterator provided by Jena

    /**
     * Constructor
     * @param path to the file
     * @throws IllegalArgumentException if the path provided points to a directory or if it doesn't exist
     */
    public StreamRDFParser(String path){
        File file = new File(path);
        //check for path
        if(!file.exists())
            throw new IllegalArgumentException("The provided path to the file does not exist");
        if(!file.isFile())
            throw new IllegalArgumentException("The provided path to the file is a directory path");
        this.path = path;

        //initialize the iterator
        this.iterator = AsyncParser.asyncParseTriples(this.path);
    }

    /**
     * @return true if the iterator has a next triple, else false
     */
    public boolean hasNext(){
        return iterator.hasNext();
    }

    /**
     * @return the next statement as a map
     * @throws NoSuchElementException if there are no other triples
     */
    public StreamRDFParser.CustomTriple next(){
        if(!iterator.hasNext()){
            throw new NoSuchElementException("No other triples");
        }

        //get the next triple from the Jena iterator and return
        //the augmented triple with a type associated to every element of the triple
        Triple triple = iterator.next();

        return new StreamRDFParser.CustomTriple(triple);
    }

    /**
     * This method releases all the resources and close the stream
     */
    public void close(){
        iterator.close();
    }

    public class CustomTriple {

        private Map.Entry<String, String> subject;
        private String predicate;
        private Map.Entry<String, String> object;

        /**
         * Default Constructor
         *
         * @param triple triple read by Jena stream parser
         */
        public CustomTriple(Triple triple) {
            predicate = triple.getPredicate().getLocalName();

            String subjectValue;
            if(triple.getSubject().isURI())
                subjectValue = triple.getSubject().getLocalName();
            else
                subjectValue = triple.getSubject().toString();

            String objectValue;
            if(triple.getObject().isURI())
                objectValue = triple.getObject().getLocalName();
            else
                objectValue = triple.getObject().toString();

            Node object = triple.getObject();

            //assign the correct type to the statement subject
            if (predicate.equals("type")) {
                if (object.toString().equals("Property"))
                    this.subject = new AbstractMap.SimpleEntry<>(ParsedDataset.FIELDS.PROPERTIES, subjectValue);
                if (object.toString().equals("Class"))
                    this.subject = new AbstractMap.SimpleEntry<>(ParsedDataset.FIELDS.CLASSES, subjectValue);
                if (!object.toString().equals("Class") && !object.toString().equals("Property"))
                    this.subject = new AbstractMap.SimpleEntry<>(ParsedDataset.FIELDS.ENTITIES, subjectValue);
            } else {
                this.subject = new AbstractMap.SimpleEntry<>(ParsedDataset.FIELDS.ENTITIES, subjectValue);
            }

            //assign the correct type to the statement object
            if (object.isLiteral())
                this.object = new AbstractMap.SimpleEntry<>(ParsedDataset.FIELDS.LITERALS ,object.getLiteral().toString());
            else
                this.object = new AbstractMap.SimpleEntry<>(ParsedDataset.FIELDS.ENTITIES, objectValue);

        }

        public Map.Entry<String, String> getSubject() {
            return subject;
        }

        public String getPredicate() {
            return predicate;
        }

        public Map.Entry<String, String> getObject() {
            return object;
        }

    }


    //ONLY FOR DEBUG PURPOSE
    public static void main(String[] args){
        //RDFParser parser = new RDFParser("/home/manuel/Tesi/ACORDAR/Test/dataset-50/wappen.rdf");
        StreamRDFParser parser = new StreamRDFParser("/home/manuel/Tesi/ACORDAR/Test/dataset-1/curso.ttl");

        int i = 0;
        while(parser.hasNext()){
            if(true){
                StreamRDFParser.CustomTriple triple = parser.next();
                System.out.print(triple.getSubject().getValue()+":"+triple.getSubject().getKey()+"     ");
                System.out.print(triple.getPredicate()+"     ");
                System.out.print(triple.getObject().getValue()+":"+triple.getObject().getKey()+"     "+"\n");
                i++;
            } else {
                break;
            }
        }
    }
}
