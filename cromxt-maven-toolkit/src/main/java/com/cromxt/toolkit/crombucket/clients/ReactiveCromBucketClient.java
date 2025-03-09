package com.cromxt.toolkit.crombucket.clients;

import com.cromxt.toolkit.crombucket.FileVisibility;
import com.cromxt.toolkit.crombucket.response.FileUploadResponse;
import org.springframework.http.codec.multipart.FilePart;
import reactor.core.publisher.Mono;

abstract public class ReactiveCromBucketClient extends CromBucketClient {

    abstract public Mono<FileUploadResponse> saveFile(FilePart file, Long fileSize);

    abstract public Mono<FileUploadResponse> saveFile(FilePart file, Long fileSize, FileVisibility visibility);

    abstract public Mono<FileUploadResponse> deleteFile(String fileUrl);

    abstract public Mono<FileUploadResponse> changeFileVisibility(String fileUrl,FileVisibility visibility);

    abstract public Mono<FileUploadResponse> updateFile(FilePart filePart,Long fileSize ,String fileUrl);

    abstract public Mono<FileUploadResponse> updateFile(FilePart filePart,Long fileSize ,String fileUrl, FileVisibility visibility);
}
