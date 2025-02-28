package com.cromxt.toolkit.crombucket.clients.impl;

import com.cromxt.common.crombucket.routeing.BucketDetailsResponse;
import com.cromxt.common.crombucket.routeing.MediaDetails;
import com.cromxt.proto.files.*;
import com.cromxt.toolkit.crombucket.CromBucketCreadentials;
import com.cromxt.toolkit.crombucket.clients.CromBucketBlockingClient;
import com.cromxt.toolkit.crombucket.exceptions.CromBucketServerException;
import com.cromxt.toolkit.crombucket.response.FileUploadResponse;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;


@Service
@RequiredArgsConstructor
@Slf4j
public class CromBucketBlockingClientImpl implements CromBucketBlockingClient {

    private final RestClient restClient;
    private final CromBucketCreadentials clientCredentials;


    @Override
    public FileUploadResponse saveFile(MultipartFile file, Long fileSize) throws IOException {
        return initiateUploading(fileSize, false, extractExtension(file.getName()), file.getInputStream());
    }

    @Override
    public FileUploadResponse saveFile(MultipartFile file, Long fileSize, Boolean hlsStatus) throws IOException {
        return initiateUploading(fileSize, hlsStatus, extractExtension(file.getName()), file.getInputStream());
    }

    private FileUploadResponse initiateUploading(
            Long fileSize,
            Boolean hlsStatus,
            String extension,
            InputStream fileData
    ) {

        BucketDetailsResponse bucketDetails = getBucketDetails(fileSize, extension);

        ManagedChannel channel = createNettyManagedChannel(bucketDetails);

        MediaHeaders mediaHeaders = MediaHeaders.newBuilder()
                .setContentType(extension)
                .setClientSecret(clientCredentials.getClientSecret())
                .setHlsStatus(hlsStatus)
                .build();

        Metadata headers = generateHeaders(mediaHeaders);

        MediaHandlerServiceGrpc.MediaHandlerServiceStub mediaHandlerServiceStub = MediaHandlerServiceGrpc.newStub(channel).withInterceptors(
                MetadataUtils.newAttachHeadersInterceptor(headers)
        );

        List<FileUploadResponse> fileUploadResponseList = new ArrayList<>();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        StreamObserver<MediaUploadRequest> requestStreamObserver = mediaHandlerServiceStub.uploadFile(new StreamObserver<>() {
            @Override
            public void onNext(MediaUploadResponse mediaUploadResponse) {
                if (mediaUploadResponse.getStatus() == OperationStatus.ERROR) {
                    return;
                }
                MediaObjectDetails mediaObjectDetails = mediaUploadResponse.getMediaObjectDetails();
                FileUploadResponse fileUploadResponse = FileUploadResponse.builder()
                        .fileId(mediaObjectDetails.getFileId())
                        .accessUrl(mediaObjectDetails.getAccessUrl())
                        .fileSize(mediaObjectDetails.getFileSize())
                        .contentType(mediaObjectDetails.getContentType())
                        .createdOn(mediaObjectDetails.getCreatedOn())
                        .build();
                fileUploadResponseList.add(fileUploadResponse);
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("Some error occurred while saving the file with message {}", throwable.getMessage());
                countDownLatch.countDown();
            }

            @Override
            public void onCompleted() {
                channel.shutdown();
                countDownLatch.countDown();
            }
        });

        try {
            byte[] chunkData = new byte[4096];
            int length;
            while ((length = fileData.read(chunkData)) > 0) {
                requestStreamObserver.onNext(
                        MediaUploadRequest
                                .newBuilder()
                                .setFile(ByteString.copyFrom(chunkData, 0, length))
                                .build()
                );
            }
            requestStreamObserver.onCompleted();
            countDownLatch.await();
        } catch (IOException ex) {
            throw new CromBucketServerException("Error occurred while read the file from stream data.");
        } catch (InterruptedException exception) {
            throw new CromBucketServerException("Some process interrupt the thread flow");
        }

        if (fileUploadResponseList.isEmpty()) {
            throw new CromBucketServerException("Some error occurred on the Server.");
        }

        return fileUploadResponseList.get(0);
    }

    private BucketDetailsResponse getBucketDetails(Long fileSize, String fileExtension) {
        MediaDetails mediaDetails = MediaDetails.builder()
                .fileExtension(fileExtension)
                .fileSize(fileSize)
                .build();
        String url = String.format("%s/api/v1/routes", clientCredentials.getBaseUrl());
        return restClient
                .post()
                .uri(URI.create(url))
                .header("Api-Key", clientCredentials.getClientSecret())
                .body(mediaDetails)
                .retrieve()
                .onStatus(HttpStatusCode::isError, (clientRequest, clientResponse) -> {
                    throw new CromBucketServerException("Some error occurred.");
                })
                .body(BucketDetailsResponse.class);
    }
}
