package com.cromxt.toolkit.crombucket.clients.impl;


import com.cromxt.common.crombucket.routeing.BucketDetailsResponse;
import com.cromxt.common.crombucket.routeing.MediaDetails;
import com.cromxt.proto.files.*;
import com.cromxt.toolkit.crombucket.CromBucketCreadentials;
import com.cromxt.toolkit.crombucket.clients.ReactiveCromBucketClient;
import com.cromxt.toolkit.crombucket.exceptions.CromBucketServerException;
import com.cromxt.toolkit.crombucket.response.FileUploadResponse;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
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
public class ReactiveCromBucketClientImpl implements ReactiveCromBucketClient {

    private final CromBucketCreadentials clientCredentials;
    private final WebClient webClient;


    @Override
    public Mono<FileUploadResponse> saveFile(FilePart file, Long fileSize) {
        return initiateUploading(fileSize, false,extractExtension(file.filename()),file.content());
    }

    @Override
    public Mono<FileUploadResponse> saveFile(FilePart file, Long fileSize, Boolean hlsStatus) {
        return initiateUploading(fileSize, hlsStatus,extractExtension(file.filename()), file.content());
    }

    private Mono<FileUploadResponse> initiateUploading(
            Long fileSize,
            Boolean hlsStatus,
            String extension,
            Flux<DataBuffer> fileData
    ){


        Mono<BucketDetailsResponse> bucketDetails = getBucketDetails(fileSize,extension);

        MediaHeaders mediaHeaders = MediaHeaders.newBuilder()
                .setClientSecret(clientCredentials.getClientSecret())
                .setExtension(extractExtension(extension))
                .setHlsStatus(hlsStatus)
                .build();

        return bucketDetails
                .flatMap(bucket ->
                        uploadFile(fileData, mediaHeaders, bucket).map(mediaObjectDetails -> FileUploadResponse.builder()
                                .fileId(mediaObjectDetails.getFileId())
                                .fileSize(mediaObjectDetails.getFileSize())
                                .accessUrl(mediaObjectDetails.getAccessUrl())
                                .contentType(mediaObjectDetails.getExtension())
                                .createdOn(mediaObjectDetails.getCreatedOn())
                                .build()
                        ));

    }


    private Mono<BucketDetailsResponse> getBucketDetails(Long fileSize, String fileExtension) {
//       TODO: Add all media details;
        MediaDetails mediaDetails = MediaDetails.builder()
                .fileSize(fileSize)
                .fileExtension(fileExtension)
                .build();
        String url = String.format("%s/api/v1/routes", clientCredentials.getBaseUrl());
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
                                                MediaHeaders mediaHeaders,
                                                BucketDetailsResponse bucketDetailsResponse) {

        ManagedChannel channel = createNettyManagedChannel(bucketDetailsResponse);

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





}
