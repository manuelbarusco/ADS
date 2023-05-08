package dei.unipd.index.thread;

import dei.unipd.analyze.AnalyzerUtil;
import dei.unipd.index.DatasetIndexer;
import dei.unipd.utils.Constants;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class IndexMain {
    public static void main(String[] args){
        final int ramBuffer = 512;
        final String indexPath = "Constants.indexPathTest";
        final String datasetDirectoryPath = Constants.datasetsDirectoryPathTest;
        final String logFilePath = Constants.indexerLogFilePath;

        final int expectedDatasets = 4;
        final String charsetName = "UTF-8";

        CharArraySet cas = AnalyzerUtil.loadStopList("/home/manuel/Tesi/EDS/EDS/eds/src/main/resources/stoplists/nltk-stopwords.txt");
        final Analyzer a = new StandardAnalyzer(cas);

        //final Similarity sim = new LMDirichletSimilarity(1800);

        IndexSetup indexSetup = new IndexSetup(a, ramBuffer, indexPath, datasetDirectoryPath,
                charsetName, expectedDatasets, logFilePath );

        indexSetup.setupAndRunIndexing();


    }
}
