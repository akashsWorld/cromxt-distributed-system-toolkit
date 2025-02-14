package com.cromxt.toolkit.cloudstore.impl;

import com.cromxt.toolkit.cloudstore.CromBucketClient;
import com.cromxt.toolkit.cloudstore.response.FileUploadResponse;

import java.io.InputStream;

public class CromBucketClientImpl implements CromBucketClient {
    @Override
    public FileUploadResponse saveFile(Long contentLength, InputStream data) {
        return FileUploadResponse.builder().build();
    }
}
