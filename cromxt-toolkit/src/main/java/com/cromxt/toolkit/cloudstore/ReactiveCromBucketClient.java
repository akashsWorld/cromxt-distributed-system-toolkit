package com.cromxt.toolkit.cloudstore;

import com.cromxt.toolkit.cloudstore.response.FileUploadResponse;
import reactor.core.publisher.Mono;

public interface ReactiveCromBucketClient {

    Mono<FileUploadResponse> saveFile(MediaObjectDataBuffer mediaObjectDataBuffer);
}
