package com.analysis.model;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Data
@ToString
@Getter
@Setter
public class TweetModelCSV implements Serializable {

    private Integer tweetNumber;
    private String tweet;
    private String negativeWords;
    private String positiveWords;
    private String polarity;

}
