package dei.unipd.index;

import com.apicatalog.jsonld.expansion.ObjectExpansion;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.jena.atlas.json.io.parser.JSONParser;

import java.io.*;

public class JSONUpdate {
    public static void main(String[] args){
        JsonElement json = null;
        try (Reader reader = new InputStreamReader(new FileInputStream("/home/manuel/Tesi/ACORDAR/Test/dataset-1/dataset-1.json"), "UTF-8")) {
            json = JsonParser.parseReader( reader );
        } catch (Exception e) {
            // do something
        }
        JsonObject object = json.getAsJsonObject();
        System.out.println(object.get("dataset_id"));
    }
}
