package com.analysis.semantic;

import com.analysis.model.NewsModel;
import com.analysis.model.NewsWordFrequency;
import com.analysis.model.TweetModel;
import com.analysis.regex.RegexConstant;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SemanticAnalysis {

    public void performSemanticAnalysis() {

        System.out.println("Analysing News data for Semantic Analysis");

        /* Creating Spark session */
        SparkSession spark = SparkSession
                .builder().config("spark.testing.memory", "471859200")
                .appName("Sentiment and Semantic Analysis")
                .master("local[*]")
                .getOrCreate();


        /* News data file location */
        String jsonPath = "src/main/resources/news_raw.json";

        /* Creating RDD from news json file */
        JavaRDD<NewsModel> newsDataSet = spark.read().json(jsonPath).as(Encoders.bean(NewsModel.class)).toJavaRDD();

        /** Creating a List of NewsModel.Articles the news data RDD
         * @see com.analysis.model.NewsModel
         * */
        List<List<NewsModel.Articles>> list = newsDataSet.map(newsData -> newsData.getArticles()).collect();


        /** Flattening the list of news articles into a single List of type NewsModel.Articles.
         * This will capture the title, content and  description from News API response.
         *
         * @see com.analysis.model.NewsModel.Articles
         * */
        List<NewsModel.Articles> articlesList = list
                .stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());

        System.out.println("Number of news documents to analyse: " + articlesList.size());


        //List of words used to query the articles
        List<String> queryList = Arrays.asList("Canada", "Halifax", "University", "Dalhousie University", "Business");

        List<NewsWordFrequency> newsWordFrequencyList = new ArrayList<>();

        //Calling computeFrequency for the given query.
        queryList.forEach(query -> {
            newsWordFrequencyList.add(computeFrequency(query, articlesList));
        });


        // Converting the list to CSV compatible model
        List<NewsWordFrequency> documentList = convertWordFrequencyList(newsWordFrequencyList);

        // Converting the list to JavaRDD and writing to output file
        Dataset<Row> rowDataset = spark.createDataFrame(documentList, NewsWordFrequency.class);
        Dataset<Row> finalDataSet = rowDataset.select("totalDocuments", "searchQuery", "documentFrequency", "frequencyPerTotalDocuments", "inverseDocumentFrequency");
        finalDataSet.coalesce(1).write().option("header", "true").mode(SaveMode.Overwrite).csv("output/news-semantic-analysis/TF-IDF");


        // Fetching only Canada keyword record from the computed list.
        List<NewsWordFrequency> documentFrequencyList = newsWordFrequencyList.stream().filter(item -> {
            if (item.getSearchQuery() != null && item.getSearchQuery().equalsIgnoreCase("canada")) {
                return true;
            }
            return false;
        }).collect(Collectors.toList());


        /**
         * Code block for for calculating frequency of word occurrence, total words in the record and 
         * Calculating the highest frequency.
         */
        if (!documentFrequencyList.isEmpty()) {
            documentFrequencyList.forEach(item -> {

                // Converting the list to JavaRDD and writing to output file
                List<NewsWordFrequency.FrequencyCount> frequencyCountList = item.getFrequencyCount();
                Dataset<Row> frequencyDataset = spark.createDataFrame(frequencyCountList, NewsWordFrequency.FrequencyCount.class);
                frequencyDataset.coalesce(1).write().option("header", "true").mode(SaveMode.Overwrite).csv("output/news-semantic-analysis/Frequency");

                // Calculating the highest frequency and finding the article number containing the highest frequency word.
                final Double[] max = {0.0};
                AtomicReference<Integer> count = new AtomicReference<>(0);
                frequencyCountList.forEach(frequency -> {
                    Double f = frequency.getFrequency();
                    Double m = frequency.getTotalWordsInArticle();

                    Double maxTemp = Double.valueOf(f / m);

                    if (maxTemp > max[0]) {
                        max[0] = maxTemp;
                        count.set(frequency.getArticleNumber());
                    }
                });

                System.out.println("max: " + max[0]);
                System.out.println("count: " + count.get());
                System.out.println("News article with frequently occurring word is: " + articlesList.get(count.get()));

                /** Converting the data to CSV compatible model
                 *
                 * @see com.analysis.model.NewsModel.Articles
                 */
                NewsModel.Articles articles = articlesList.get(count.get());
                List<NewsModel.Articles> articlesList1 = new ArrayList<>();
                articlesList1.add(articles);

                // Converting the list to JavaRDD and writing to output file
                Dataset<Row> articleTextFile = spark.createDataFrame(articlesList1, NewsModel.Articles.class);
                articleTextFile.coalesce(1).write().option("header", "true").mode(SaveMode.Overwrite).csv("output/news-semantic-analysis/article");

            });
        }
    }

    /**
     * Method for calculating document frequency (TF) and inverse document frequency (IDF)
     * <p>
     * This method a word and list of articles in which the word needs to be queried.
     * A regex to remove special characters is applied to clean the data.
     * <p>
     * It computes TF and IDF, and stores the data in form of NewsWordFrequency object for the given word.
     *
     * @see com.analysis.model.NewsWordFrequency
     * @see TweetModel
     */
    private NewsWordFrequency computeFrequency(String word, List<NewsModel.Articles> articlesList) {

        word = word.toLowerCase();
        NewsWordFrequency newsWordFrequency = new NewsWordFrequency();

        List<NewsWordFrequency.FrequencyCount> frequencyCountList = new ArrayList<>();

        newsWordFrequency.setTotalDocuments(articlesList.size());
        newsWordFrequency.setSearchQuery(word);

        AtomicReference<Integer> documentFrequency = new AtomicReference<>(0);
        AtomicReference<Integer> documentNumber = new AtomicReference<>(0);

        String finalWord = word;
        articlesList.forEach(article -> {

            Double totalWords = 0.0;
            Double frequency = 0.0;
            if (article != null) {

                // Considering only content, description and title from NewsAPI response.
                String wholeString = (article.getContent() + " " + article.getDescription() + " " + article.getTitle()).toLowerCase();

                // Removing special characters
                wholeString = wholeString.replaceAll(RegexConstant.ALPHANUMERIC_FILTER, " ");

                totalWords = Double.valueOf(wholeString.split("\\s+").length);


                //Computing and setting document frequency (TF)
                if (wholeString.contains(finalWord)) {
                    documentNumber.getAndSet(documentNumber.get() + 1);
                    documentFrequency.getAndSet(documentFrequency.get() + 1);

                    Pattern p = Pattern.compile(finalWord);
                    Matcher m = p.matcher(wholeString);

                    //Counting the frequency of the word in the given article.
                    while (m.find()) {
                        frequency++;
                    }

                    NewsWordFrequency.FrequencyCount frequencyCount = new NewsWordFrequency.FrequencyCount();

                    frequencyCount.setArticleNumber(documentNumber.get());
                    frequencyCount.setFrequency(frequency);
                    frequencyCount.setTotalWordsInArticle(totalWords);

                    frequencyCountList.add(frequencyCount);
                }
            }
        });
        newsWordFrequency.setFrequencyCount(frequencyCountList);
        newsWordFrequency.setDocumentFrequency(documentFrequency.get());

        //Computing and setting inverse document frequency (IDF)
        if (documentFrequency.get() > 0) {
            newsWordFrequency.setFrequencyPerTotalDocuments((double) (newsWordFrequency.getTotalDocuments() / documentFrequency.get()));
            newsWordFrequency.
                    setInverseDocumentFrequency(Math.floor(Math.log10(newsWordFrequency.getFrequencyPerTotalDocuments()) * 100) / 100);
        }
        return newsWordFrequency;
    }

    /**
     * Method for converting list of NewsWordFrequency for storing the data CSV format.
     *
     * @see com.analysis.model.NewsWordFrequency
     */
    private List<NewsWordFrequency> convertWordFrequencyList(List<NewsWordFrequency> newsWordFrequencyList) {
        List<NewsWordFrequency> documentList = new ArrayList<>();
        newsWordFrequencyList.forEach(item -> {
            NewsWordFrequency newsWordFrequency = new NewsWordFrequency();
            newsWordFrequency.setInverseDocumentFrequency(item.getInverseDocumentFrequency());
            newsWordFrequency.setDocumentFrequency(item.getDocumentFrequency());
            newsWordFrequency.setSearchQuery(item.getSearchQuery());
            newsWordFrequency.setTotalDocuments(item.getTotalDocuments());
            newsWordFrequency.setFrequencyPerTotalDocuments(item.getFrequencyPerTotalDocuments());

            documentList.add(newsWordFrequency);

        });
        return documentList;
    }
}