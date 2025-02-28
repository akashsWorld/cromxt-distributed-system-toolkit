package com.cromxt.toolkit.crombucket.clients;

import com.cromxt.toolkit.crombucket.response.FileUploadResponse;
import org.springframework.web.multipart.MultipartFile;

public interface CromBucketClient {

    FileUploadResponse saveFile(MultipartFile file, Long contentLength);
}
