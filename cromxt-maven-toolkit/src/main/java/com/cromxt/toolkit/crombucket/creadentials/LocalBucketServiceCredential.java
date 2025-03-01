package com.cromxt.toolkit.crombucket.creadentials;

import com.cromxt.toolkit.crombucket.CromBucketCreadentials;


public class LocalBucketServiceCredential implements CromBucketCreadentials {

    private final String baseUrl;
    private final String clientSecret;

    public LocalBucketServiceCredential(
            String baseUrl
    ) {
        this.baseUrl = baseUrl;
        this.clientSecret = "long-client-secret";
    }

    public LocalBucketServiceCredential(
            Integer bucketPort
    ) {
        this.baseUrl = String.format("http://localhost:%s", bucketPort);
        this.clientSecret = "local-client-secret";
    }

    @Override
    public String getBaseUrl() {
        return baseUrl;
    }

    @Override
    public String getClientSecret() {
        return clientSecret;
    }
}
