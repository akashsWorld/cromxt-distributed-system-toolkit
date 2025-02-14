package com.cromxt.toolkit.cloudstore;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.InputStream;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class MediaObjectInputStream {
    private String contentType;
    private Boolean hlsStatus;
    private InputStream data;
}
