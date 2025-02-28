package com.cromxt.toolkit.crombucket.clients;

import com.cromxt.toolkit.crombucket.response.FileUploadResponse;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Mono;

public interface ReactiveCromBucketClient {

    Mono<FileUploadResponse> saveFile(FilePart file, Long contentLength);

    Mono<FileUploadResponse> saveFile(FilePart file, Long contentLength, Boolean hlsStatus);
}
