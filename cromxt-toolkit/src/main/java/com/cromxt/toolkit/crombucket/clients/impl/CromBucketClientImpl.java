package com.cromxt.toolkit.crombucket.clients.impl;

import com.cromxt.toolkit.crombucket.clients.CromBucketClient;
import com.cromxt.toolkit.crombucket.response.FileUploadResponse;
import org.springframework.web.multipart.MultipartFile;

public class CromBucketClientImpl implements CromBucketClient {

    @Override
    public FileUploadResponse saveFile(MultipartFile file, Long contentLength) {
        return null;
    }
}
