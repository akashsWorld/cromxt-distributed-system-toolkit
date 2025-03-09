package com.cromxt.toolkit.crombucket.clients;

import com.cromxt.toolkit.crombucket.FileVisibility;
import com.cromxt.toolkit.crombucket.response.FileUploadResponse;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

abstract public class CromBucketWebClient extends CromBucketClient {

    abstract public FileUploadResponse saveFile(MultipartFile file) throws IOException;
    abstract public FileUploadResponse saveFile(MultipartFile file, FileVisibility fileVisibility) throws IOException;
    abstract public FileUploadResponse deleteFile(String fileUrl);
    abstract public FileUploadResponse changeFileVisibility(String fileUrl, FileVisibility visibility);
    abstract public FileUploadResponse updateFile(String fileUrl, MultipartFile file) throws IOException;
    abstract public FileUploadResponse updateFile(String fileUrl, MultipartFile file, FileVisibility visibility) throws IOException;
}
