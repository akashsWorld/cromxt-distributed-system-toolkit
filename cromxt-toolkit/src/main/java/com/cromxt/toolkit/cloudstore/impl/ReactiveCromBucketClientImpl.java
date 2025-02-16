package com.cromxt.toolkit.cloudstore.impl;


import com.cromxt.grpc.MediaHeadersKey;
import com.cromxt.proto.files.*;
import com.cromxt.common.crombucket.routeing.BucketDetails;
import com.cromxt.common.crombucket.routeing.MediaDetails;
import com.cromxt.toolkit.cloudstore.CromxtUserDetails;
import com.cromxt.toolkit.cloudstore.ReactiveCromBucketClient;
import com.cromxt.toolkit.cloudstore.exceptions.CloudStoreServerException;
import com.cromxt.toolkit.cloudstore.MediaObjectDataBuffer;
import com.cromxt.toolkit.cloudstore.response.FileUploadResponse;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


public class ReactiveCromBucketClientImpl implements ReactiveCromBucketClient {


    private final String API_KEY;
    private final WebClient webClient;

    public ReactiveCromBucketClientImpl(CromxtUserDetails cromxtUserDetails, WebClient.Builder webclientBuilder) {
        this.API_KEY = cromxtUserDetails.getApiKey();
        this.webClient = webclientBuilder.baseUrl("http://localhost:8904/routing/api/v1/routes/get-bucket-id")
                .build();
    }

    @Override
    public Mono<FileUploadResponse> saveFile(MediaObjectDataBuffer mediaObjectDataBuffer) {

        Mono<BucketDetails> bucketDetails = getBucketDetails();

        MediaHeaders mediaHeaders = MediaHeaders.newBuilder()
                .setContentType(mediaObjectDataBuffer.getContentType())
                .setContentLength(mediaObjectDataBuffer.getContentLength())
                .setHlsStatus(mediaObjectDataBuffer.getHlsStatus())
                .setApiKey(API_KEY)
                .build();

        return bucketDetails.flatMap(bucket -> uploadFile(mediaObjectDataBuffer.getMedia(), mediaHeaders, bucket).map(accessUrl -> FileUploadResponse.builder()
                .fileId(accessUrl)
                .build()));
    }


    private Mono<BucketDetails> getBucketDetails() {
//       TODO: Add all media details;
        MediaDetails mediaDetails = new MediaDetails();

        return webClient
                .post()
                .header("Api-Key", API_KEY)
                .bodyValue(mediaDetails)
                .retrieve()
                .onStatus(HttpStatusCode::isError, (clientResponse -> Mono.error(new CloudStoreServerException("Some error occurred."))))
                .bodyToMono(BucketDetails.class);
    }


    private Mono<String> uploadFile(Flux<DataBuffer> fileData,
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
//        return data.as(reactorMediaHandlerServiceStub::uploadFile)
//                .flatMap(fileUploadResponse -> {
//                    channel.shutdown();
//                    if (fileUploadResponse.getStatus() == OperationStatus.ERROR) {
//                        return Mono.error(new ClientException(fileUploadResponse.getErrorMessage()));
//                    }
//
//                    return Mono.just(fileUploadResponse.getFileName());
//                });

        return reactorMediaHandlerServiceStub.uploadFile(data).flatMap(fileUploadResponse -> {
            channel.shutdown();
            if (fileUploadResponse.getStatus() == OperationStatus.ERROR) {
                return Mono.error(new CloudStoreServerException(fileUploadResponse.getErrorMessage()));
            }

            return Mono.just(fileUploadResponse.getFileName());
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


}
