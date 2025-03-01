package com.cromxt.toolkit.crombucket.clients;

import com.cromxt.toolkit.crombucket.response.FileUploadResponse;
import org.springframework.http.codec.multipart.FilePart;
import reactor.core.publisher.Mono;

public interface ReactiveCromBucketClient extends CromBucketClient{

    Mono<FileUploadResponse> saveFile(FilePart file, Long fileSize);
    Mono<FileUploadResponse> saveFile(FilePart file, Long fileSize, Boolean hlsStatus);
}
