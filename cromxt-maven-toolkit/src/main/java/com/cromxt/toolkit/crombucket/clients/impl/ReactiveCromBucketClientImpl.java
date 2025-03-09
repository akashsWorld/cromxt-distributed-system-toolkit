package com.cromxt.toolkit.crombucket.clients.impl;


import com.cromxt.common.crombucket.routeing.BucketDetailsResponse;
import com.cromxt.common.crombucket.routeing.MediaDetails;
import com.cromxt.proto.files.*;
import com.cromxt.toolkit.crombucket.CromBucketCreadentials;
import com.cromxt.toolkit.crombucket.FileVisibility;
import com.cromxt.toolkit.crombucket.clients.ReactiveCromBucketClient;
import com.cromxt.toolkit.crombucket.exceptions.CromBucketServerException;
import com.cromxt.toolkit.crombucket.response.FileUploadResponse;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
public class ReactiveCromBucketClientImpl extends ReactiveCromBucketClient {

    private final CromBucketCreadentials clientCredentials;
    private final WebClient webClient;


    @Override
    public Mono<FileUploadResponse> saveFile(FilePart file, Long fileSize) {
        return initiateUploading(FileVisibility.PUBLIC, fileSize, extractExtension(file.filename()), file.content());
    }

    @Override
    public Mono<FileUploadResponse> saveFile(FilePart file, Long fileSize, FileVisibility visibility) {
        return initiateUploading(visibility, fileSize, extractExtension(file.filename()), file.content());
    }

    @Override
    public Mono<FileUploadResponse> deleteFile(String fileUrl) {
        URI uri = URI.create(fileUrl);
        return webClient
                .delete()
                .uri(uri)
                .header("Authorization", clientCredentials.getClientSecret())
                .retrieve()
                .onStatus(HttpStatusCode::isError, clientResponse -> Mono.error(new CromBucketServerException("Some error occurred.")))
                .bodyToMono(FileUploadResponse.class);
    }

    @Override
    public Mono<FileUploadResponse> changeFileVisibility(String fileUrl, FileVisibility visibility) {
        URI uri = URI.create(fileUrl);
        return webClient
                .patch()
                .uri(uri)
                .bodyValue(new HashMap<>(Map.of("visibility", visibility.name())))
                .header("Authorization", clientCredentials.getClientSecret())
                .retrieve()
                .onStatus(HttpStatusCode::isError, clientResponse -> Mono.error(new CromBucketServerException("Some error occurred.")))
                .bodyToMono(FileUploadResponse.class);
    }

    @Override
    public Mono<FileUploadResponse> updateFile(FilePart filePart, Long fileSize, String fileUrl) {
        return initiateUpdate(filePart, fileSize, fileUrl, FileVisibility.PUBLIC);
    }

    @Override
    public Mono<FileUploadResponse> updateFile(FilePart filePart, Long fileSize, String fileUrl, FileVisibility visibility) {
        return initiateUpdate(filePart,fileSize,fileUrl,visibility);
    }


    private Mono<FileUploadResponse> initiateUpdate(FilePart filePart, Long fileSize, String fileUrl, FileVisibility visibility) {
        Mono<FileUploadResponse> deletedFile = deleteFile(fileUrl);
        String extension = extractExtension(filePart.filename());
        return deletedFile.flatMap(ignored->initiateUploading(visibility,fileSize,extension,filePart.content()));
    }

    private Mono<FileUploadResponse> initiateUploading(
            FileVisibility visibility,
            Long fileSize,
            String extension,
            Flux<DataBuffer> fileData
    ) {
        Mono<BucketDetailsResponse> bucketDetails = getBucketDetails(fileSize);

        Metadata metadata = generateHeaders(extension, clientCredentials.getClientSecret(), visibility);


        return bucketDetails
                .flatMap(bucket ->
                        uploadFile(fileData, metadata, bucket).map(mediaObjectDetails -> FileUploadResponse.builder()
                                .mediaId(mediaObjectDetails.getMediaId())
                                .fileSize(mediaObjectDetails.getFileSize())
                                .accessUrl(mediaObjectDetails.getAccessUrl())
                                .visibility(getVisibility(mediaObjectDetails.getVisibility()))
                                .build()
                        ));

    }


    private Mono<BucketDetailsResponse> getBucketDetails(Long fileSize) {
//       TODO: Add all media details;
        String url = String.format("%s/api/v1/routes", clientCredentials.getBaseUrl());

        MediaDetails mediaDetails = MediaDetails.builder()
                .fileSize(fileSize)
                .build();

        return webClient
                .post()
                .uri(URI.create(url))
                .header("Api-Key", clientCredentials.getClientSecret())
                .bodyValue(mediaDetails)
                .retrieve()
                .onStatus(HttpStatusCode::isError, (clientResponse -> Mono.error(new CromBucketServerException("Some error occurred."))))
                .bodyToMono(BucketDetailsResponse.class);
    }


    private Mono<MediaObjectDetails> uploadFile(Flux<DataBuffer> fileData,
                                                Metadata metadata,
                                                BucketDetailsResponse bucketDetailsResponse) {

        ManagedChannel channel = createNettyManagedChannel(bucketDetailsResponse);

        // Use of reactive implementation instead of blocking.

        ReactorFileHandlerServiceGrpc.ReactorFileHandlerServiceStub reactorFileHandlerServiceStub = ReactorFileHandlerServiceGrpc
                .newReactorStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));

        Flux<FileUploadRequest> data = fileData
                .map(dataBuffer -> {
                    byte[] bytes = new byte[dataBuffer.readableByteCount()];
                    dataBuffer.read(bytes);
                    DataBufferUtils.release(dataBuffer);
                    return FileUploadRequest
                            .newBuilder()
                            .setFile(ByteString.copyFrom(bytes))
                            .build();
                });

        return reactorFileHandlerServiceStub
                .uploadFile(data)
                .doOnError(err -> MediaUploadResponse.newBuilder().setStatus(OperationStatus.ERROR).setErrorMessage(err.getMessage()).build())
                .flatMap(mediaUploadResponse -> {
                    channel.shutdown();
                    if (mediaUploadResponse.getStatus() == OperationStatus.ERROR) {
                        return Mono.error(new CromBucketServerException(mediaUploadResponse.getErrorMessage()));
                    }
                    return Mono.just(mediaUploadResponse.getMediaObjectDetails());
                });

    }

}
