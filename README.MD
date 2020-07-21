# Sentiment and Semantic analysis of Twitter and NewsAPI data using Apache Spark

SentimentAndSemanticAnalysis is a Java project built using Apache Spark API 
for performing Sentiment and Semantic analysis of Twitter and NewsAPI data.
 
## Installation

Import this project in Intellij/eclipse to clone from `https://git.cs.dal.ca/mpanchal/data-assignment-4.git`

Using [gradle](https://gradle.org/install/), build the project by running following command:

```bash
gradle clean install
```

## Prerequisite

This project requires a local setup of [Apache Spark](https://spark.apache.org/downloads.html). 

It should be running on a standalone [1 master-1 worker setup mode](https://spark.apache.org/docs/latest/spark-standalone.html).

Start the master and slave using the following command:

`./sbin/start-master.sh`

`./sbin/start-slave.sh <master-spark-URL>`


## Run the project
```
Run the main() of the com.analysis.App.java
```

OR

Execute the gradle command in terminal/command line.

```
gradle run
```


## Input files
The Twitter data: `tweets_data.json` is located and retrieved in JSON format from the `src/main/resources` directory.

The NewsAPI data: `news_raw.json` is located and retrieved in JSON format from the `src/main/resources` directory.

List of positive and negative words used for Sentiment analysis can be found at `src/main/resources/positive-words.txt` 
and `src/main/resources/negative-words.txt` respectively.

## Operations

#### Sentiment analysis

For code: refer the file: `com.analysis.sentiment.SentimentAnalysis.java`

Lexicon based Sentiment analysis is performed on Twitter data. 
List of positive and negative words are taken from [cs.uic.edu](https://www.cs.uic.edu/~liub/FBS/sentiment-analysis.html)

1. **Text** attribute from Twitter data is cleaned to remove special characters and RT keyword using the regex:
  `[^A-Za-z]+` and 
  `RT][^\\s]+` .

2. A bag of word for each text field is created and for each token, the classification of positive or negative word is derived using from the positive and negative text files.

3.  Polarity is computed for each tweet by comparing the positive and negative counter.

4. The output file in CSV format is generated under the `output/twitter-sentiment-analysis ` directory.

#### Semantic analysis

For code: refer the file: `com.analysis.semantic.SentimentAnalysis.java`

Term frequency–inverse document frequency (TF-IDF) is calculated for the NewsAPI data.


1. The news json dataset in unwind to form a a list of articles. The list contains approximately 1100 news articles

2. **Topic**, **Content** and **Descrption** attributes from NewsAPI data is cleaned to remove special characters using the regex:
  `[^A-Za-z]+`.
  
3.  TF-IDF is calculated for the words: `"Canada", "Halifax", "University", "Dalhousie University", "Business"`.
The output file in CSV format is generated under the `output/news-semantic-analysis/TF-IDF ` directory.

4.  Frequency (f) of the word: `Canada` is computed for each news article (total words in the article: w). 
The output file in CSV format is generated under the `output/news-semantic-analysis/Frequency ` directory.

5. The document containing the highest occurrence of the word : `Canada` is computed and printed on console.
The output file in text format, containing the article is generated under the `output/news-semantic-analysis/article ` directory.



## Library reference
[Spark Project Core 3.0.0-preview2](https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.12/3.0.0-preview2)

[Spark Project SQL 3.0.0-preview2](https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.12/3.0.0-preview2)
