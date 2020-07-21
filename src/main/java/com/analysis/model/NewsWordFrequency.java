package com.analysis.model;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;


@Data
@ToString
@Getter
@Setter
public class NewsWordFrequency implements Serializable {

    private Integer totalDocuments;
    private String searchQuery;
    private Integer documentFrequency;
    private Double frequencyPerTotalDocuments;
    private Double inverseDocumentFrequency;
    private List<FrequencyCount> frequencyCount;

    @Data
    @ToString
    @Getter
    @Setter
    public static class FrequencyCount implements Serializable {

        private Integer articleNumber;
        private Double totalWordsInArticle;
        private Double frequency;

    }
}
