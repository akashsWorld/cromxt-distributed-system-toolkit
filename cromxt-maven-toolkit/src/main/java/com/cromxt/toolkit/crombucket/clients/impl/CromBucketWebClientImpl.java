package com.cromxt.toolkit.crombucket.clients.impl;

import com.cromxt.common.crombucket.routeing.BucketDetailsResponse;
import com.cromxt.common.crombucket.routeing.MediaDetails;
import com.cromxt.proto.files.*;
import com.cromxt.toolkit.crombucket.CromBucketCreadentials;
import com.cromxt.toolkit.crombucket.FileVisibility;
import com.cromxt.toolkit.crombucket.clients.CromBucketWebClient;
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
import org.springframework.web.client.RestClient;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.*;
import java.util.concurrent.CountDownLatch;


@Slf4j
@RequiredArgsConstructor
public class CromBucketWebClientImpl extends CromBucketWebClient {

    private final CromBucketCreadentials clientCredentials;
    private final RestClient restClient;


    @Override
    public FileUploadResponse saveFile(MultipartFile file) throws IOException {
        file.getContentType();
        return initiateUploading(FileVisibility.PUBLIC, file.getSize(), extractExtension(Objects.requireNonNull(file.getOriginalFilename())), file.getInputStream());
    }

    @Override
    public FileUploadResponse saveFile(MultipartFile file, FileVisibility fileVisibility) throws IOException {
        return initiateUploading(fileVisibility, file.getSize(), extractExtension(Objects.requireNonNull(file.getOriginalFilename())), file.getInputStream());
    }

    @Override
    public FileUploadResponse deleteFile(String fileUrl) throws CromBucketServerException {
        URI uri = URI.create(fileUrl);
        return restClient
                .delete()
                .uri(uri)
                .header("Authorization", clientCredentials.getClientSecret())
                .retrieve()
                .onStatus(HttpStatusCode::isError, (clientRequest, clientResponse) -> {
                    throw new CromBucketServerException("Some error occurred");
                })
                .body(FileUploadResponse.class);
    }

    @Override
    public FileUploadResponse changeFileVisibility(String fileUrl, FileVisibility visibility) {
        URI uri = URI.create(fileUrl);
        return restClient
                .patch()
                .uri(uri)
                .body(new HashMap<>(Map.of("visibility", visibility.name())))
                .header("Authorization", clientCredentials.getClientSecret())
                .retrieve()
                .onStatus(HttpStatusCode::isError, (request, response) -> {
                    throw new CromBucketServerException("Some error occurred.");
                })
                .body(FileUploadResponse.class);
    }

    @Override
    public FileUploadResponse updateFile(String fileUrl, MultipartFile file) throws IOException {
        return null;
    }

    @Override
    public FileUploadResponse updateFile(String fileUrl, MultipartFile file, FileVisibility visibility) throws IOException {
        return null;
    }

    private FileUploadResponse initiateUpdate(
            String fileUrl,
            MultipartFile file,
            FileVisibility visibility
    ) {
        deleteFile(fileUrl);
        FileUploadResponse fileUploadResponse = null;
        try {

            fileUploadResponse = initiateUploading(visibility, file.getSize(), extractExtension(Objects.requireNonNull(file.getOriginalFilename())), file.getInputStream());
        } catch (IOException ioException) {
            log.error("Error occurred while update the file with message {}", ioException.getMessage());
        }

        return fileUploadResponse;
    }

    private FileUploadResponse initiateUploading(
            FileVisibility visibility,
            Long fileSize,
            String extension,
            InputStream fileData
    ) {

        BucketDetailsResponse bucketDetails = getBucketDetails(fileSize);

        ManagedChannel channel = createNettyManagedChannel(bucketDetails);

        Metadata headers = generateHeaders(extension, clientCredentials.getClientSecret(), visibility);

        FileHandlerServiceGrpc.FileHandlerServiceStub fileHandlerGrpcStub = FileHandlerServiceGrpc.newStub(channel).withInterceptors(
                MetadataUtils.newAttachHeadersInterceptor(headers)
        );

        List<FileUploadResponse> fileUploadResponseList = new ArrayList<>();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        StreamObserver<FileUploadRequest> requestStreamObserver = fileHandlerGrpcStub.uploadFile(new StreamObserver<>() {
            @Override
            public void onNext(MediaUploadResponse mediaUploadResponse) {
                if (mediaUploadResponse.getStatus() == OperationStatus.ERROR) {
                    return;
                }
                MediaObjectDetails mediaObjectDetails = mediaUploadResponse.getMediaObjectDetails();
                FileVisibility savedFileVisibility = getVisibility(mediaObjectDetails.getVisibility());
                FileUploadResponse fileUploadResponse = FileUploadResponse.builder()
                        .mediaId(mediaObjectDetails.getMediaId())
                        .accessUrl(mediaObjectDetails.getAccessUrl())
                        .fileSize(mediaObjectDetails.getFileSize())
                        .visibility(savedFileVisibility)
                        .build();
                fileUploadResponseList.add(fileUploadResponse);
            }

            @Override
            public void onError(Throwable throwable) {
                throwable.fillInStackTrace();
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
                        FileUploadRequest
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

    private BucketDetailsResponse getBucketDetails(Long fileSize) {
        String url = String.format("%s/api/v1/routes", clientCredentials.getBaseUrl());
        MediaDetails mediaDetails = MediaDetails.builder()
                .fileSize(fileSize)
                .build();
        return restClient
                .post()
                .uri(URI.create(url))
                .header("Authorization", clientCredentials.getClientSecret())
                .body(mediaDetails)
                .retrieve()
                .onStatus(HttpStatusCode::isError, (clientRequest, clientResponse) -> {
                    throw new CromBucketServerException("Some error occurred.");
                })
                .body(BucketDetailsResponse.class);
    }

    private Long getFileSize(MultipartFile file) {
        return file.getSize();
    }
}
