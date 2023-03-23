package search;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

/**
 * @author Riccardo Forzan (riccardo.forzan@studenti.unipd.it);
 */
public class QueryExpander {

    //Stores all the synonyms found
    private static final HashMap<String, List<String>> dictionary = new HashMap<>();
    private static final Rake rake = new Rake();

    /**
     * Given a query recognizes the most important piece, then generates all the queries derived by the one given
     * using synonyms
     *
     * @param query              string to expand
     * @param allTokens          boolean parameter that indicates if we want to generate synonyms for every token or only for the main token
     * @param maxSynonymsPerWord number of synonyms to generate for every key token
     * @param threshold          score threshold used in the synonym filtering, 0 if you want all the synonyms, max value is 1
     * @return a list of queries derived from the one above
     * @throws IOException if any error occurs while using the network
     */
    public static List<String> generateAllExpandedQueries(String query, boolean allTokens, int maxSynonymsPerWord, double threshold) throws IOException {

        if (threshold < 0 || threshold > 1) {
            throw new IllegalArgumentException("Value threshold is expected to be in the interval [0-1]");
        }

        if(maxSynonymsPerWord < 0){
            throw new IllegalArgumentException("Value maxSynonymsPerWord is expected to be >= 0");
        }

        //Recognize the most important passage of the sentence using RAKE
        Map<String, Double> results = rake.getKeywordsFromText(query);
        String msk = Collections.max(results.entrySet(), Comparator.comparingDouble(Map.Entry::getValue)).getKey();

        //Split the most important passage of the query into individual tokens
        var tokens = msk.split("\\s+");

        //Stores all the queries generated
        ArrayList<String> expandedQueries = new ArrayList<>();

        if (allTokens) {
            //Find synonyms for each main token
            for (String token : tokens) {
                List<String> synonyms = searchForSingleWordSynonym(token, threshold);

                //Generate all the possible queries by substituting the synonyms
                for (int i = 0; i < synonyms.size() && i < maxSynonymsPerWord; i++) {
                    String newQuery = query.replace(token, synonyms.get(i));
                    expandedQueries.add(newQuery);
                }

            }
        } else {
            //Find synonyms for the most important main token
            List<String> synonyms = searchForSingleWordSynonym(tokens[0], threshold);

            //Generate all the possible queries by substituting the synonyms
            for (int i = 0; i < synonyms.size() && i < maxSynonymsPerWord; i++) {
                String newQuery = query.replace(tokens[0], synonyms.get(i));
                expandedQueries.add(newQuery);
            }
        }

        return expandedQueries;
    }

    /**
     * Searches for synonyms optimizing the number of HTTP calls using an hashmap as cache
     *
     * @param wordToSearch word for which synonyms will be searched
     * @param threshold    score threshold used in the synonym filtering, 0 if you want all the synonyms, max value is 1
     * @return a List of string if some synonyms have been found, the size of the list is 0 if no synonym has been found
     * @throws IOException if any error occurs while using the network
     */
    private static List<String> searchForSingleWordSynonym(String wordToSearch, double threshold) throws IOException {

        List<String> synonyms;

        //See if the keyword has been already searched
        boolean alreadySearched = dictionary.containsKey(wordToSearch);

        //Try to match into the dictionary
        synonyms = dictionary.get(wordToSearch);

        //If no match has been found in the dictionary
        if (!alreadySearched) {
            //Invoke API call
            synonyms = apiCall(wordToSearch, threshold);
            //Add synonyms to the dictionary for future usage
            dictionary.put(wordToSearch, synonyms);
        }

        return synonyms;
    }

    /**
     * Using DataMuse API looks for a synonym of the word given as parameter
     *
     * @param wordToSearch word (single, without spaces)
     * @param threshold    score threshold used in the synonym filtering, 0 if you want all the synonyms, max value is 1
     * @return List of synonym words found by DataMuse API, the size of the list is 0 if no synonym has been found
     * @throws IOException if some error occurs while using network
     */
    private static List<String> apiCall(String wordToSearch, double threshold) throws IOException {

        //Look for synonyms
        String url = "https://api.datamuse.com/words?rel_syn=" + wordToSearch;

        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        con.setRequestMethod("GET");
        con.setRequestProperty("User-Agent", "Mozilla/5.0");

        int responseCode = con.getResponseCode();
        if (responseCode != 200)
            throw new UnsupportedOperationException();

        StringBuilder response;
        try (BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
            String inputLine;
            response = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
        }

        ObjectMapper mapper = new ObjectMapper();

        ArrayList<Word> words = mapper.readValue(response.toString(),
                mapper.getTypeFactory().constructCollectionType(ArrayList.class, Word.class));

        List<String> validSynonyms = new ArrayList<>();

        /*
         *  Normalize the result, consider as valid synonyms only
         *  the ones with at most half the score of the best synonym found
         */
        if (words.size() > 0) {
            //Sort the collection of words by their score and calculate threshold
            var best = Collections.max(words, Comparator.comparingInt(Word::getScore));
            var bestScore = best.score;

            System.out.println(threshold * bestScore);

            //Filter all valid synonyms
            var goodSynonyms = words.stream().filter(word -> word.score >= (threshold * bestScore)).toList();

            //Populate the arraylist to be returned
            for (Word syn : goodSynonyms) {
                validSynonyms.add(syn.word);
            }
        }

        return validSynonyms;
    }

    /**
     * Helper class used to map DataMuse API returned synonyms
     */
    static class Word {
        private String word;
        private int score;

        public String getWord() {
            return this.word;
        }

        public int getScore() {
            return this.score;
        }

        @Override
        public String toString() {
            return "Word{" +
                    "word='" + word + '\'' +
                    ", score=" + score +
                    '}';
        }
    }

}
