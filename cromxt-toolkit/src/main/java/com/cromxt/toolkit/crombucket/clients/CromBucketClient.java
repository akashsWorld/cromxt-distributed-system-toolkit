package com.cromxt.toolkit.crombucket.clients;

import com.cromxt.common.crombucket.grpc.MediaHeadersKey;
import com.cromxt.common.crombucket.routeing.BucketDetailsResponse;
import com.cromxt.proto.files.MediaHeaders;
import com.cromxt.toolkit.crombucket.response.FileUploadResponse;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.NettyChannelBuilder;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

public interface CromBucketClient {
    default String extractExtension(String fileName) {
        return fileName.substring(fileName.lastIndexOf(".") + 1);
    }

    default ManagedChannel createNettyManagedChannel(BucketDetailsResponse bucketDetailsResponse) {
        return NettyChannelBuilder
                .forAddress(bucketDetailsResponse.getHostName(), bucketDetailsResponse.getRpcPort())
                .usePlaintext()
                .flowControlWindow(NettyChannelBuilder.DEFAULT_FLOW_CONTROL_WINDOW)
                .build();
    }

    default Metadata generateHeaders(MediaHeaders mediaHeaders) {

        Metadata metadata = new Metadata();

        Metadata.Key<byte[]> metaDataKey = (Metadata.Key<byte[]>) MediaHeadersKey.MEDIA_META_DATA
                .getMetaDataKey();
        metadata.put(metaDataKey, mediaHeaders.toByteArray());
        return metadata;
    }
}
