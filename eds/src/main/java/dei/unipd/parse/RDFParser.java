package dei.unipd.parse;

import org.apache.jena.ontology.OntClass;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.util.FileManager;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;


/**
 * @author Manuel Barusco
 * This class use Apache Jena in order to read and extract info from RDF files
 * such as .ttl and .rdf files
 */
public class RDFParser {
    private Model graph;        //Jena object for the graph file
    private String path;        //String with the path to the rdf or ttl file

    /**
     * Constructor
     * @param path to the file
     */
    public RDFParser(String path){
        this.path = path;
        graph = ModelFactory.createDefaultModel();
        graph.read(path);
    }

    /**
     * Method that extracts all the classes present in the ttl file
     */
    public void getClasses(){
        String queryString =
                "\n" +
                "SELECT DISTINCT ?class WHERE{ \n" +
                    "?s a ?class .\n" +
                "}";

        Query query = QueryFactory.create(queryString);
        QueryExecution qexec = QueryExecutionFactory.create(query, graph);

        ResultSet results = qexec.execSelect();
        if(results.hasNext()) {
            System.out.println("has results!");
        }
        else {
            System.out.println("No Results!");
        }

        while(results.hasNext()) {
            QuerySolution soln = results.nextSolution();
            System.out.println(soln);
        }


    }

    public void getEntities(){
        String queryString =
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n" +
                "SELECT DISTINCT ?label WHERE{ \n" +
                    "?s a ?class .\n" +
                    "?s rdfs:label ?label .\n" +
                "}";

        Query query = QueryFactory.create(queryString);
        QueryExecution qexec = QueryExecutionFactory.create(query, graph);

        ResultSet results = qexec.execSelect();
        if(results.hasNext()) {
            System.out.println("has results!");
        }
        else {
            System.out.println("No Results!");
        }

        while(results.hasNext()) {
            QuerySolution soln = results.nextSolution();
            System.out.println(soln);
        }
    }

    public void getLiterals(){
        var iterator = graph.listObjects();
        while(iterator.hasNext()){
            RDFNode node = iterator.next();
            if(node.isLiteral())
                System.out.println(node.asLiteral());
        }
    }

    public void getProperties(){
        String queryString =
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n" +
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n" +
                "SELECT DISTINCT ?p WHERE{ \n" +
                    "?s ?p ?o .\n" +
                "}";

        Query query = QueryFactory.create(queryString);
        QueryExecution qexec = QueryExecutionFactory.create(query, graph);

        ResultSet results = qexec.execSelect();
        if(results.hasNext()) {
            System.out.println("has results!");
        }
        else {
            System.out.println("No Results!");
        }

        while(results.hasNext()) {
            QuerySolution soln = results.nextSolution();
            Model m = results.getResourceModel();
            System.out.println(soln);
        }
    }


    public static void main(String[] args){
        //RDFParser parser = new RDFParser("/home/manuel/Tesi/ACORDAR/Datasets/dataset-1/file-1.ttl");

        //parser.getLiterals();

        Path path = FileSystems.getDefault().getPath("/home/manuel/Tesi/ACORDAR/Datasets/dataset-1/", "file-1");
        try {
            String mimeType = Files.probeContentType(path);
            System.out.println(mimeType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
