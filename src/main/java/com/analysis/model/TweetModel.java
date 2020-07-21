package com.analysis.model;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;


@Data
@ToString
@Getter
@Setter
public class TweetModel implements Serializable {

    private Integer tweetNumber;
    private Map<String, Long> tweetBagOfWords;
    private Map<String, Long> negativeWordsMatch;
    private Map<String, Long> positiveWordsMatch;
    private String polarity;


}
