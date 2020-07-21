package com.analysis;

import com.analysis.semantic.SemanticAnalysis;
import com.analysis.sentiment.SentimentAnalysis;
import org.apache.spark.sql.SparkSession;

public class App {

    public static void main(String[] args) {


        SentimentAnalysis sentimentAnalysis = new SentimentAnalysis();
        sentimentAnalysis.performSentimentAnalysis();

        SemanticAnalysis semanticAnalysis = new SemanticAnalysis();
        semanticAnalysis.performSemanticAnalysis();
    }
}
