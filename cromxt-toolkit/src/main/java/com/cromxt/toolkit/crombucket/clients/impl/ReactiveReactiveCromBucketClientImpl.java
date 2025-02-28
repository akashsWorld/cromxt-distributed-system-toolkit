package com.cromxt.toolkit.crombucket.clients.impl;


import com.cromxt.common.crombucket.grpc.MediaHeadersKey;
import com.cromxt.common.crombucket.routeing.BucketDetails;
import com.cromxt.common.crombucket.routeing.MediaDetails;
import com.cromxt.proto.files.*;
import com.cromxt.toolkit.crombucket.CromBucketCreadentials;
import com.cromxt.toolkit.crombucket.clients.ReactiveCromBucketClient;
import com.cromxt.toolkit.crombucket.exceptions.CromBucketServerException;
import com.cromxt.toolkit.crombucket.response.FileUploadResponse;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;

@Service
@Lazy
@RequiredArgsConstructor
public class ReactiveReactiveCromBucketClientImpl implements ReactiveCromBucketClient {

    private final CromBucketCreadentials clientCredentials;
    private final WebClient webClient;


    @Override
    public Mono<FileUploadResponse> saveFile(FilePart file, Long contentLength) {
        String extension = extractExtension(file.filename());
        return initiateUploading(extension, false, file.content());
    }

    @Override
    public Mono<FileUploadResponse> saveFile(FilePart file, Long contentLength, Boolean hlsStatus) {
        return initiateUploading(file.filename(), hlsStatus, file.content());
    }

    private Mono<FileUploadResponse> initiateUploading(
            String contentType,
            Boolean hlsStatus,
            Flux<DataBuffer> fileData
    ){

        Mono<BucketDetails> bucketDetails = getBucketDetails();

        MediaHeaders mediaHeaders = MediaHeaders.newBuilder()
                .setClientSecret(clientCredentials.getClientSecret())
                .setContentType(extractExtension(contentType))
                .setHlsStatus(hlsStatus)
                .build();

        return bucketDetails
                .flatMap(bucket ->
                        uploadFile(fileData, mediaHeaders, bucket).map(mediaObjectDetails -> FileUploadResponse.builder()
                                .fileId(mediaObjectDetails.getFileId())
                                .fileSize(mediaObjectDetails.getFileSize())
                                .accessUrl(mediaObjectDetails.getAccessUrl())
                                .contentType(mediaObjectDetails.getContentType())
                                .createdOn(mediaObjectDetails.getCreatedOn())
                                .build()
                        ));

    }


    private Mono<BucketDetails> getBucketDetails() {
//       TODO: Add all media details;
        MediaDetails mediaDetails = new MediaDetails();
        String url = String.format("%s/api/v1/routes", clientCredentials.getBaseUrl());
        return webClient
                .post()
                .uri(URI.create(url))
                .header("Api-Key", clientCredentials.getClientSecret())
                .bodyValue(mediaDetails)
                .retrieve()
                .onStatus(HttpStatusCode::isError, (clientResponse -> Mono.error(new CromBucketServerException("Some error occurred."))))
                .bodyToMono(BucketDetails.class);
    }


    private Mono<MediaObjectDetails> uploadFile(Flux<DataBuffer> fileData,
                                                MediaHeaders mediaHeaders,
                                                BucketDetails bucketDetails) {

        ManagedChannel channel = createNettyManagedChannel(bucketDetails);

        Metadata headers = generateHeaders(mediaHeaders);
        // Use of reactive implementation instead of blocking.

        ReactorMediaHandlerServiceGrpc.ReactorMediaHandlerServiceStub reactorMediaHandlerServiceStub = ReactorMediaHandlerServiceGrpc
                .newReactorStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));

        Flux<MediaUploadRequest> data = fileData
                .map(dataBuffer -> {
                    byte[] bytes = new byte[dataBuffer.readableByteCount()];
                    dataBuffer.read(bytes);
                    DataBufferUtils.release(dataBuffer);
                    return MediaUploadRequest
                            .newBuilder()
                            .setFile(ByteString.copyFrom(bytes))
                            .build();
                });
      /*

        * This is another way to handle upload the data;

        return data.as(reactorMediaHandlerServiceStub::uploadFile)
                .flatMap(fileUploadResponse -> {
                    channel.shutdown();
                    if (fileUploadResponse.getStatus() == OperationStatus.ERROR) {
                        return Mono.error(new ClientException(fileUploadResponse.getErrorMessage()));
                    }

                    return Mono.just(fileUploadResponse.getFileName());
                });
       */

        return reactorMediaHandlerServiceStub
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

    private ManagedChannel createNettyManagedChannel(BucketDetails bucketDetails) {
        return NettyChannelBuilder
                .forAddress(bucketDetails.getHostName(), bucketDetails.getRpcPort())
                .usePlaintext()
                .flowControlWindow(NettyChannelBuilder.DEFAULT_FLOW_CONTROL_WINDOW)
                .build();
    }

    private ManagedChannel createManagedChannel(BucketDetails bucketDetails) {
        return ManagedChannelBuilder.forAddress(bucketDetails.getHostName(), bucketDetails.getRpcPort())
                .usePlaintext()
                .build();
    }


    private Metadata generateHeaders(MediaHeaders mediaHeaders) {

        Metadata metadata = new Metadata();

        Metadata.Key<byte[]> metaDataKey = (Metadata.Key<byte[]>) MediaHeadersKey.MEDIA_META_DATA
                .getMetaDataKey();
        metadata.put(metaDataKey, mediaHeaders.toByteArray());
        return metadata;
    }

    private MediaHandlerServiceGrpc.MediaHandlerServiceStub getMediaHandlerServiceStub(
            BucketDetails bucketDetails) {

        // To use reactive types this Stub is not used. but it can also be used as a blocking stub.
        ManagedChannel channel = ManagedChannelBuilder.forAddress(
                        bucketDetails.getHostName(),
                        bucketDetails.getRpcPort())
                .usePlaintext()
                .build();
        return MediaHandlerServiceGrpc.newStub(channel);
    }

    private String extractExtension(String extension) {
        return extension.substring(extension.lastIndexOf(".") + 1);
    }

}
