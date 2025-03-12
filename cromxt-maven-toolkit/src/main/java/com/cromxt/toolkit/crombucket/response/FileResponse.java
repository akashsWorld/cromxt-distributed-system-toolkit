package com.cromxt.toolkit.crombucket.response;


import com.cromxt.toolkit.crombucket.FileVisibility;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class FileResponse {
    private String mediaId;
    private String accessUrl;
    private Long fileSize;
    private FileVisibility visibility;
}
