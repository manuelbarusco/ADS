package dei.unipd.parse;

import org.apache.commons.io.FilenameUtils;
import org.apache.jena.rdf.model.*;

import java.io.File;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;


/**
 * @author Manuel Barusco
 * This class use Apache Jena in order to read and extract info from RDF files
 * such as .ttl and .rdf files
 */
public class RDFParser implements Iterator<RDFParser.CustomTriple> {
    private Model graph;             //Jena object for the graph file
    private final String path;             //String with the path to the given file
    private StmtIterator iterator;   //Iterator over the triples

    /**
     * Constructor
     * @param path to the file
     * @throws IllegalArgumentException if the path provided points to a directory of if it doesn't exist
     */
    public RDFParser(String path){
        File file = new File(path);
        //check for path
        if(!file.exists() || !file.isFile())
            throw new IllegalArgumentException("The provided path for the file does not exist or is a directory path");
        this.path = path;

        //check for the owl extension or else
        if(FilenameUtils.getExtension(path).equals("owl"))
            graph = ModelFactory.createOntologyModel();
        else
            graph = ModelFactory.createDefaultModel();

        //create the model
        graph.read(path);

        SimpleSelector selector = new SimpleSelector(null, null, (RDFNode)null) {
            public boolean selects(Statement s)
            { return true; }
        };

        //create the iterator
        iterator = graph.listStatements(selector);

    }

    /**
     * @return true if the iterator has a next statement, else false
     */
    public boolean hasNext(){
        return iterator.hasNext();
    }

    /**
     * @return the next statement as a map
     * @throws NoSuchElementException if there are not other statements
     */
    public CustomTriple next(){
        if(!iterator.hasNext()){
            throw new NoSuchElementException("No other statements");
        }

        //get the next statement from the jena iterator
        Statement stmt = iterator.nextStatement();

        return new CustomTriple(stmt);
    }

    /**
     * This method releases all the resources
     */
    public void close(){
        iterator.close();
        graph.close();
    }

    public class CustomTriple {

        private Map.Entry<String, String> subject;
        private String predicate;
        private Map.Entry<String, String> object;

        /**
         * Default Constructor
         *
         * @param stmt statement read by Jena
         */
        public CustomTriple(Statement stmt) {
            predicate = stmt.getPredicate().getLocalName();

            String subjectValue = stmt.getSubject().getLocalName();

            RDFNode object = stmt.getObject();

            //assign the correct type to the statement subject
            if (predicate.equals("type")) {
                if (object.toString().equals("Property"))
                    this.subject = new AbstractMap.SimpleEntry<>("properties", subjectValue);
                if (object.toString().equals("Class"))
                    this.subject = new AbstractMap.SimpleEntry<>("classes", subjectValue);
                if (predicate.equals("type") && !object.toString().equals("Class") && !object.toString().equals("Property"))
                    this.subject = new AbstractMap.SimpleEntry<>("entities", subjectValue);
            } else {
                this.subject = new AbstractMap.SimpleEntry<>("entities", subjectValue);
            }

            //assign the correct type to the statement object
            if (object.isLiteral())
                this.object = new AbstractMap.SimpleEntry<>("literals", stmt.getString());
            else if (object.isURIResource())
                this.object = new AbstractMap.SimpleEntry<>("entities", ((Resource) object).getLocalName());
            else
                this.object = new AbstractMap.SimpleEntry<>("entities", object.toString());
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
        RDFParser parser = new RDFParser("/home/manuel/Tesi/ACORDAR/Test/dataset-1/curso_sf_dump.ttl");

        int i = 0;
        while(parser.hasNext()){
            if(i<20){
                CustomTriple triple = parser.next();
                System.out.print(triple.getSubject().getValue()+"     ");
                System.out.print(triple.getPredicate()+"     ");
                System.out.print(triple.getObject().getValue()+"\n");
                i++;
            } else {
                break;
            }

        }
    }
}
