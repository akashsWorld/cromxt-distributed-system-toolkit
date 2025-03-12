package com.cromxt.toolkit.crombucket.clients;

import com.cromxt.toolkit.crombucket.FileVisibility;
import com.cromxt.toolkit.crombucket.UpdateFileVisibilityRequest;
import com.cromxt.toolkit.crombucket.response.FileResponse;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.util.List;

abstract public class CromBucketWebClient extends CromBucketClient {

    abstract public FileResponse saveFile(MultipartFile file) throws IOException;

    abstract public FileResponse saveFile(MultipartFile file, FileVisibility fileVisibility) throws IOException;

    abstract public void deleteFile(String mediaId);

    abstract public void deleteMany(List<String> mediaIds);

    abstract public FileResponse changeFileVisibility(UpdateFileVisibilityRequest updateFileVisibilityRequest);

    abstract public List<FileResponse> changeFileVisibility(List<UpdateFileVisibilityRequest> updateFileVisibilityRequests);

    abstract public FileResponse updateFile(String mediaId, MultipartFile file) throws IOException;

    abstract public FileResponse updateFile(String mediaId, MultipartFile file, FileVisibility visibility) throws IOException;
}
