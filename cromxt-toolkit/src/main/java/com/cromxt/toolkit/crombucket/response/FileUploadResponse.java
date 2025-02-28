package com.cromxt.toolkit.crombucket.response;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class FileUploadResponse {
    private String fileId;
    private String accessUrl;
    private String contentType;
    private Long fileSize;
    private String createdOn;
}
