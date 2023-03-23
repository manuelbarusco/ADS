package search;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.lucene.benchmark.quality.QualityQuery;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * Represents a topic parser
 *
 * @author Nicola Rizzetto (nicola.rizzetto.2@studenti.unipd.it)
 * @version 1.00
 * @since 1.00
 */
public class ToucheTopicsReader {

    /**
     * Returns a list of {@code QualityQuery} representing the topics
     *
     * @param in the {@code BufferedReader}
     * @return a list of {@code QualityQuery}
     * @throws IOException if something goes wrong
     */
    public QualityQuery[] readQueries(BufferedReader in) throws IOException {
        XmlMapper TopicMapper = new XmlMapper();
        List<QualityQuery> queryList = new ArrayList<>();
        Topic[] topics = TopicMapper.readValue(in, Topic[].class);
        for (Topic T : topics) {
            HashMap<String, String> fields = new HashMap<>();
            String queryID = T.getNumber();
            fields.put(ToucheSearcher.TOPIC_FIELDS.TITLE, T.getTitle());
            fields.put(ToucheSearcher.TOPIC_FIELDS.DESCRIPTION, T.getDescription());
            fields.put(ToucheSearcher.TOPIC_FIELDS.NARRATIVE, T.getNarrative());
            queryList.add(new QualityQuery(queryID, fields));
            //System.out.printf(t.getNumber() + "\n"  +t.getTitle() + "\n" + t.getDescription() + "\n" + t.getNarrative() + "\n");
        }

        QualityQuery[] Topics = new QualityQuery[queryList.size()];
        queryList.toArray(Topics);
        return Topics;
    }

    /**
     * Class for the various fields of a topic
     */
    static class Topic {

        private String number;

        private String title;

        private String description;

        private String narrative;


        public String getNumber() {
            return number;
        }

        public String getTitle() {
            return title;
        }

        public String getDescription() {
            return description;
        }

        public String getNarrative() {
            return narrative;
        }
    }
}