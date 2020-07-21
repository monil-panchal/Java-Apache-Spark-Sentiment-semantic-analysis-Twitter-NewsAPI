package com.analysis.sentiment;

import com.analysis.model.TweetModel;
import com.analysis.model.TweetModelCSV;
import com.analysis.regex.RegexConstant;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SentimentAnalysis {

    public void performSentimentAnalysis() {

        System.out.println("Analysing Twitter data for Sentiment Analysis");

        /* Creating Spark session */
        SparkSession spark = SparkSession
                .builder().config("spark.testing.memory", "471859200")
                .appName("Sentiment and Semantic Analysis")
                .master("local[*]")
                .getOrCreate();

        /* Tweet data, positive, and negative words file location */
        String jsonPath = "src/main/resources/tweets_data.json";
        String positiveFilePath = "src/main/resources/positive-words.txt";
        String negativeFilePath = "src/main/resources/negative-words.txt";

        /* Creating RDD from tweets_data json file */
        JavaRDD<Row> tweetDataSet = spark.read().json(jsonPath).toJavaRDD();

        System.out.println("Number of tweets to analyse: " + tweetDataSet.count());

        /* Creating RDD from positive and negative text files */
        JavaRDD<String> positiveDataSet = spark.read().textFile(positiveFilePath).toJavaRDD();
        JavaRDD<String> negativeDataSet = spark.read().textFile(negativeFilePath).toJavaRDD();

        /* Creating Map from positive RDD */
        Map<String, String> positiveWordMap = positiveDataSet
                .collect()
                .stream()
                .collect(Collectors.toMap(item -> item, item -> item));

        /* Creating Map from positive RDD */
        Map<String, String> negativeWordMap = negativeDataSet
                .collect()
                .stream()
                .collect(Collectors.toMap(item -> item, item -> item));


        /**  Creating Java List of TweetModel Model
         * @see TweetModel
         * After extracting text field from the data source,
         * this method removes RT keyword and special characters from each Tweet.
         *
         * This method also tokenizes each tweet text into Bag of words using HashMap data structure.
         * */
        List<TweetModel> tweetList = tweetDataSet
                .map(item -> {
                    String tweetText = item.getAs("text");

                    String filteredText = null;
                    Map<String, Long> wordFreq = null;
                    TweetModel tweet = new TweetModel();

                    // Filtering text field from the data source to remove special characters and RT keyword.
                    if (tweetText != null && !tweetText.isEmpty() && tweetText.length() >= 2) {
                        filteredText = tweetText
                                .replaceAll(RegexConstant.ALPHANUMERIC_FILTER, " ")
                                .replaceAll(RegexConstant.RT_FILTER, " ");

                        Stream<String> stream = Stream.of(filteredText.split(" ")).parallel();

                        // Creating a bag of word for the text field.
                        // This contains the frequency of words in a particular tweet.
                        wordFreq = stream
                                .filter(string -> {
                                    if (string.isEmpty())
                                        return false;
                                    return true;
                                }).collect(Collectors.groupingBy(String::toString, Collectors.counting()));

                        tweet = new TweetModel();
                        tweet.setTweetBagOfWords(wordFreq);

                        return tweet;
                    }
                    return tweet;
                })
                .filter(data -> data.getTweetBagOfWords() != null)
                .collect();

        /** Calling the calculatePolarity()
         * to find the occurrences of positive and negative words for each bag of words of the tweets
         * and computing polarity.
         * */
        tweetList = calculatePolarity(tweetList, positiveWordMap, negativeWordMap);


        // Converting the list to CSV compatible model
        List<TweetModelCSV> tweetCSVList = convertTweetModel(tweetList);

        // Converting the list to JavaRDD and writing to output file
        Dataset<Row> rowDataset = spark.createDataFrame(tweetCSVList, TweetModelCSV.class);
        Dataset<Row> finalDataSet = rowDataset.select("tweetNumber", "tweet", "positiveWords", "negativeWords", "polarity");
        finalDataSet.coalesce(1).write().option("header", "true").mode(SaveMode.Overwrite).csv("output/twitter-sentiment-analysis");

        System.out.println("Output file is generated in the project directory: output/twitter-sentiment-analysis");

        spark.close();

    }


    /**
     * Method for counting the number of positive and negative words for a given list of Tweets.
     * This method takes a map of positive and negative words , matches them for each bag of words for the given tweets,
     * and stores the frequency of each positive and negative words in a HashMap.
     * <p>
     * Storing the frequency of positive and negative tweets
     * in HashMap provides extensibility and efficient look up.
     * <p>
     * This method also computes the polarity of each tweets by comparing the number of positive and negative words found in the tweet.
     *
     * @see TweetModel
     */
    private List<TweetModel> calculatePolarity(List<TweetModel> tweetList, Map<String, String> positiveWordList, Map<String, String> negativeWordList) {
        AtomicReference<Integer> tweetCounter = new AtomicReference<>(0);

        tweetList.forEach(tweet -> {

            if (tweet != null && tweet.getTweetBagOfWords() != null && !tweet.getTweetBagOfWords().isEmpty()) {
                tweetCounter.set(tweetCounter.get() + 1);
                tweet.setTweetNumber(tweetCounter.get());

                Map<String, Long> positiveWordCount = new HashMap<>();
                Map<String, Long> negativeWordCount = new HashMap<>();

                AtomicReference<Integer> positiveWordCounter = new AtomicReference<>(0);
                AtomicReference<Integer> negativeWordCounter = new AtomicReference<>(0);

                // For each word in the bag of words,
                // finding the occurrences of positive and negative words and storing them in HashMap.
                tweet.getTweetBagOfWords().keySet().stream().forEach(key -> {

                    if (key != null && !key.isEmpty()) {
                        if (positiveWordList.containsKey(key)) {
                            positiveWordCount.put(key, positiveWordCount.getOrDefault(key + 1, 1L));

                            // Increasing the positive word counter
                            positiveWordCounter.getAndSet(positiveWordCounter.get() + 1);
                        }
                        tweet.setPositiveWordsMatch(positiveWordCount);
                        if (negativeWordList.containsKey(key)) {
                            negativeWordCount.put(key, negativeWordCount.getOrDefault(key + 1, 1L));

                            // Increasing the positive word counter
                            negativeWordCounter.getAndSet(negativeWordCounter.get() + 1);
                        }
                        tweet.setNegativeWordsMatch(negativeWordCount);
                    }
                });

                // Computing the polarity of the tweet
                // During the above retrieval step, this counters incremented for each occurrence of positive and negative words
                if (positiveWordCounter.get() == negativeWordCounter.get())
                    tweet.setPolarity("Neutral");
                else if (negativeWordCounter.get() > positiveWordCounter.get())
                    tweet.setPolarity("Negative");
                else
                    tweet.setPolarity("Positive");
            }
        });

        return tweetList;
    }


    /**
     * Method for converting list of TweetModel to list of TweetModelCSV for storing the data CSV format.
     * As CSV cannot contain Array/List/Map, this method converts each attribute of
     *
     * @see com.analysis.model.TweetModel
     * to
     * @see com.analysis.model.TweetModelCSV containing the data in String form.
     */
    private List<TweetModelCSV> convertTweetModel(List<TweetModel> tweetList) {
        List<TweetModelCSV> tweetCSVList = tweetList.stream()
                .map(tweet -> {
                    TweetModelCSV tweetCSV = new TweetModelCSV();
                    if (tweet != null) {
                        tweetCSV = new TweetModelCSV();
                        tweetCSV.setTweetNumber(tweet.getTweetNumber());
                        if (tweet.getTweetBagOfWords() != null && tweet.getTweetBagOfWords().size() > 0) {
                            StringBuilder tweetMessage = new StringBuilder();

                            tweet.getTweetBagOfWords().forEach((k, v) -> {
                                tweetMessage.append(k + " ");
                            });
                            tweetCSV.setTweet(tweetMessage.toString());
                        }

                        if (tweet.getPositiveWordsMatch() != null && tweet.getPositiveWordsMatch().size() > 0) {
                            tweetCSV.setPositiveWords(tweet.getPositiveWordsMatch().keySet().toString());
                        }

                        if (tweet.getNegativeWordsMatch() != null && tweet.getNegativeWordsMatch().size() > 0) {
                            tweetCSV.setNegativeWords(tweet.getNegativeWordsMatch().keySet().toString());
                        }

                        tweetCSV.setPolarity(tweet.getPolarity());
                    }
                    return tweetCSV;
                })
                .collect(Collectors.toList());

        return tweetCSVList;
    }
}