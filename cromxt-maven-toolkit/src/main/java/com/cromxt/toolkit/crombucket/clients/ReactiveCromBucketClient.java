package com.cromxt.toolkit.crombucket.clients;

import com.cromxt.toolkit.crombucket.FileVisibility;
import com.cromxt.toolkit.crombucket.UpdateFileVisibilityRequest;
import com.cromxt.toolkit.crombucket.response.FileResponse;
import org.springframework.http.codec.multipart.FilePart;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

abstract public class ReactiveCromBucketClient extends CromBucketClient {

    abstract public Mono<FileResponse> saveFile(FilePart file, Long fileSize);

    abstract public Mono<FileResponse> saveFile(FilePart file, Long fileSize, FileVisibility visibility);

    abstract public Mono<Void> delete(String mediaId);

    abstract public Mono<Void> deleteMany(List<String> mediaIds);

    abstract public Mono<FileResponse> changeFileVisibility(UpdateFileVisibilityRequest updateFileVisibilityRequest);

    abstract public Flux<FileResponse> changeFileVisibility(List<UpdateFileVisibilityRequest> updateFileVisibilityRequest);

    abstract public Mono<FileResponse> updateFile(String mediaId, FilePart filePart, Long fileSize);

    abstract public Mono<FileResponse> updateFile(String mediaId, FilePart filePart, Long fileSize, FileVisibility visibility);
}
