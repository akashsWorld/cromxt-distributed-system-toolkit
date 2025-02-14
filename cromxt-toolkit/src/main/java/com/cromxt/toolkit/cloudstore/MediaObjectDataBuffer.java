package com.cromxt.toolkit.cloudstore;


import lombok.*;
import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
@Data
public class MediaObjectDataBuffer {
    private String contentType;
    private Boolean hlsStatus;
    private Flux<DataBuffer> data;
    private Long contentLength;
}
