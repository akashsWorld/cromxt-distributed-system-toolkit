package com.cromxt.toolkit.crombucket.clients;

import com.cromxt.common.crombucket.grpc.MediaHeadersKey;
import com.cromxt.common.crombucket.routeing.StorageServerAddress;
import com.cromxt.proto.files.FileMetadata;
import com.cromxt.proto.files.Visibility;
import com.cromxt.toolkit.crombucket.FileVisibility;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.NettyChannelBuilder;
import lombok.NonNull;

abstract public class CromBucketClient {
    protected static final String MEDIA_MANAGER_URL = "media-manager/api/v1/medias";
    protected static final String BUCKET_MANAGER_URL = "bucket-manager/api/v1/buckets/fetch-storage-address";

    protected String extractExtension(@NonNull String fileName) {
        return fileName.substring(fileName.lastIndexOf(".") + 1);
    }

    protected ManagedChannel createNettyManagedChannel(StorageServerAddress bucketDetailsResponse) {
        return NettyChannelBuilder
                .forAddress(bucketDetailsResponse.getHostName(), bucketDetailsResponse.getRpcPort())
                .usePlaintext()
                .flowControlWindow(NettyChannelBuilder.DEFAULT_FLOW_CONTROL_WINDOW)
                .build();
    }

    protected Metadata generateHeaders(String extension, String clientSecret, FileVisibility visibility) {

        FileMetadata mediaHeaders = FileMetadata.newBuilder()
                .setExtension(extension)
                .setClientSecret(clientSecret)
                .setVisibility(getVisibility(visibility))
                .build();

        Metadata metadata = new Metadata();

        Metadata.Key<byte[]> metaDataKey = (Metadata.Key<byte[]>) MediaHeadersKey.FILE_METADATA
                .getMetaDataKey();
        metadata.put(metaDataKey, mediaHeaders.toByteArray());
        return metadata;
    }

    protected Visibility getVisibility(FileVisibility fileVisibility) {
        return switch (fileVisibility) {
            case PUBLIC -> Visibility.PUBLIC;
            case PRIVATE -> Visibility.PRIVATE;
            case PROTECTED -> Visibility.PROTECTED;
        };
    }
    protected FileVisibility getVisibility(Visibility visibility){
        return switch (visibility) {
            case PUBLIC -> FileVisibility.PUBLIC;
            case PRIVATE -> FileVisibility.PRIVATE;
            case PROTECTED -> FileVisibility.PROTECTED;
            case UNRECOGNIZED -> throw new IllegalArgumentException("Invalid visibility");
        };
    }


}
