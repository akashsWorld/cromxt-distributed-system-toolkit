package com.cromxt.toolkit.crombucket;


import lombok.*;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class UpdateFileVisibilityRequest {
    private String mediaId;
    private FileVisibility visibility;
}
