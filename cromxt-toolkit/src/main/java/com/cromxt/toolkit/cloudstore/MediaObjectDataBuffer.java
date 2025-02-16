package com.cromxt.toolkit.cloudstore;


import lombok.*;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.codec.multipart.FilePart;
import reactor.core.publisher.Flux;


@ToString
@Data
public class MediaObjectDataBuffer {
    private String contentType;
    private Boolean hlsStatus;
    private Flux<DataBuffer> media;
    private String fileName;
    private Long contentLength;

    public static MediaObjectDataBuffer MediaObjectDataBufferBuilder(FilePart media,Long contentLength,Boolean hlsStatus) {
        return new MediaObjectDataBuffer(media,contentLength,hlsStatus);
    }

    private MediaObjectDataBuffer(FilePart media,Long contentLength,boolean hlsStatus) {
        this.media = media.content();
        this.fileName = media.filename();
        this.hlsStatus = hlsStatus;
        this.contentType = getFileExtension(media.filename());
        this.contentLength = contentLength;
    }

    private String getFileExtension(String fileName) {
        if (fileName == null || fileName.lastIndexOf(".") == -1) {
            return ""; // No extension found
        }
        return fileName.substring(fileName.lastIndexOf(".") + 1);
    }
}
