// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.common.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manager the file uploaded.
 * This file manager is currently only used to manage files
 * uploaded through the Upload RESTFul API.
 * And limit the number and size of the maximum upload file.
 * It can also browse or delete files through the RESTFul API.
 */
public class TmpFileMgr {
    public static final Logger LOG = LogManager.getLogger(TmpFileMgr.class);

    private static final long MAX_TOTAL_FILE_SIZE_BYTES = 1 * 1024 * 1024 * 1024L; // 1GB
    private static final long MAX_TOTAL_FILE_NUM = 100;
    public static final long MAX_SINGLE_FILE_SIZE = 100 * 1024 * 1024L; // 100MB
    private static final String UPLOAD_DIR = "_doris_upload";

    private AtomicLong fileIdGenerator = new AtomicLong(0);
    private String rootDir;
    private Map<Long, TmpFile> fileMap = Maps.newConcurrentMap();

    private long totalFileSize = 0;

    public TmpFileMgr(String dir) {
        this.rootDir = dir + "/" + UPLOAD_DIR;
        init();
    }

    private void init() {
        File root = new File(rootDir);
        if (!root.exists()) {
            root.mkdirs();
        } else if (!root.isDirectory()) {
            throw new IllegalStateException("Path " + rootDir + " is not directory");
        }

        // delete all files under this dir at startup.
        // This means that all uploaded files will be lost after FE restarts.
        // This is just for simplicity.
        Util.deleteDirectory(root);
    }

    /**
     * Simply used `synchronized` to allow only one user upload file at one time.
     * So that we can easily control the number of files and total size of files.
     *
     * @param uploadFile
     * @return
     * @throws TmpFileException
     */
    public synchronized TmpFile upload(UploadFile uploadFile) throws TmpFileException {
        if (uploadFile.file.getSize() > MAX_SINGLE_FILE_SIZE) {
            throw new TmpFileException("File size " + uploadFile.file.getSize() + " exceed limit " + MAX_SINGLE_FILE_SIZE);
        }

        if (totalFileSize + uploadFile.file.getSize() > MAX_TOTAL_FILE_SIZE_BYTES) {
            throw new TmpFileException("Total file size will exceed limit " + MAX_TOTAL_FILE_SIZE_BYTES);
        }

        if(fileMap.size() > MAX_TOTAL_FILE_NUM) {
            throw new TmpFileException("Number of temp file " + fileMap.size() + " exceed limit " + MAX_TOTAL_FILE_NUM);
        }

        long fileId = fileIdGenerator.incrementAndGet();
        String fileUUID = UUID.randomUUID().toString();

        TmpFile tmpFile = new TmpFile(fileId, fileUUID, uploadFile.file.getOriginalFilename(),
                uploadFile.file.getSize(), uploadFile.columnSeparator);
        try {
            tmpFile.save(uploadFile.file);
        } catch (IOException e) {
            throw new TmpFileException("Failed to upload file. Reason: " + e.getMessage());
        }
        fileMap.put(tmpFile.id, tmpFile);
        totalFileSize += uploadFile.file.getSize();
        return tmpFile;
    }

    public TmpFile getFile(long id, String uuid) throws TmpFileException {
        TmpFile tmpFile = fileMap.get(id);
        if (tmpFile == null || !tmpFile.uuid.equals(uuid)) {
            throw new TmpFileException("File with [" + id + "-" + uuid + "] does not exist");
        }
        return tmpFile;
    }

    public List<TmpFile> listFiles() {
        return Lists.newArrayList(fileMap.values());
    }

    public class TmpFile {
        public final long id;
        public final String uuid;
        public final String originFileName;
        public final long fileSize;
        public String absPath;
        public String columnSeparator;
        public List<List<String>> lines = null;
        public int maxColNum = 0;

        private static final int MAX_PREVIEW_LINES = 10;

        public TmpFile(long id, String uuid, String originFileName, long fileSize, String columnSeparator) {
            this.id = id;
            this.uuid = uuid;
            this.originFileName = originFileName;
            this.fileSize = fileSize;
            this.columnSeparator = columnSeparator;
        }

        public void save(MultipartFile file) throws IOException {
            File dest = new File(Joiner.on("/").join(rootDir, uuid));
            boolean uploadSucceed = false;
            try {
                file.transferTo(dest);
                this.absPath = dest.getAbsolutePath();
                uploadSucceed = true;
                LOG.info("upload file {} succeed at {}", this, dest.getAbsolutePath());
            } catch (IOException e) {
                LOG.warn("failed to upload file {}, dest: {}", this, dest.getAbsolutePath(), e);
                throw e;
            } finally {
                if (!uploadSucceed) {
                    dest.delete();
                }
            }
        }

        public void setPreview() throws IOException {
            lines = Lists.newArrayList();
            try (FileReader fr = new FileReader(absPath);
                 BufferedReader bf = new BufferedReader(fr)) {
                String str;
                while ((str = bf.readLine()) != null) {
                    String[] cols = str.split(columnSeparator);
                    lines.add(Lists.newArrayList(cols));
                    if (cols.length > maxColNum) {
                        maxColNum = cols.length;
                    }
                    if (lines.size() >= MAX_PREVIEW_LINES) {
                        break;
                    }
                }
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[id=").append(id).append(", uuid=").append(uuid).append("， origin name=").append(originFileName)
                    .append(", size=").append(fileSize).append("]");
            return sb.toString();
        }
    }

    public static class UploadFile {
        public MultipartFile file;
        public String columnSeparator;

        public UploadFile(MultipartFile file, String columnSeparator) {
            this.file = file;
            this.columnSeparator = columnSeparator;
        }
    }

    public static class TmpFileException extends Exception {
        public TmpFileException(String msg) {
            super(msg);
        }

        public TmpFileException(String msg, Throwable t) {
            super(msg, t);
        }
    }
}

