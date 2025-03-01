package com.cromxt.toolkit.crombucket.clients;

import com.cromxt.toolkit.crombucket.response.FileUploadResponse;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

public interface CromBucketBlockingClient extends CromBucketClient {

    FileUploadResponse saveFile(MultipartFile file, Long fileSize) throws IOException;

    FileUploadResponse saveFile(MultipartFile file, Long fileSize, Boolean hlsStatus) throws IOException;
}
