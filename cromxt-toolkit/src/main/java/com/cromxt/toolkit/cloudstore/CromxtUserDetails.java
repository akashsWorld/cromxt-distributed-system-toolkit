package com.cromxt.toolkit.cloudstore;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.web.client.RestClient;
import org.springframework.web.reactive.function.client.WebClient;

@Getter
@AllArgsConstructor
public class CromxtUserDetails {
    private String apiKey;
}
