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
public class NewsModel implements Serializable {

    private List<Articles> articles;

    @Data
    @ToString
    @Getter
    @Setter
    public static class Articles implements Serializable {
        private String title;
        private String content;
        private String description;

    }
}