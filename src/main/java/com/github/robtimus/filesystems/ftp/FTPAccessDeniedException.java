/*
 * FTPAccessDeniedException.java
 * Copyright 2016 Rob Spoor
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.robtimus.filesystems.ftp;

import java.nio.file.AccessDeniedException;

/**
 * An exception that is thrown if an FTP command does not execute successfully because access to a file has been denied.
 *
 * @author Rob Spoor
 */
public class FTPAccessDeniedException extends AccessDeniedException implements FTPResponse {

    private static final long serialVersionUID = -8991690858093027964L;

    private final int replyCode;

    /**
     * Creates a new {@code FTPAccessDeniedException}.
     *
     * @param file A string identifying the file, or {@code null} if not known.
     * @param replyCode The integer value of the reply code of the last FTP reply that triggered this exception.
     * @param replyString The entire text from the last FTP response that triggered this exception. It will be used as the exception's reason.
     */
    public FTPAccessDeniedException(String file, int replyCode, String replyString) {
        super(file, null, replyString);
        this.replyCode = replyCode;
    }

    /**
     * Creates a new {@code FTPAccessDeniedException}.
     *
     * @param file A string identifying the file, or {@code null} if not known.
     * @param other A string identifying the other file, or {@code null} if not known.
     * @param replyCode The integer value of the reply code of the last FTP reply that triggered this exception.
     * @param replyString The entire text from the last FTP response that triggered this exception. It will be used as the exception's reason.
     */
    public FTPAccessDeniedException(String file, String other, int replyCode, String replyString) {
        super(file, other, replyString);
        this.replyCode = replyCode;
    }

    @Override
    public int getReplyCode() {
        return replyCode;
    }

    @Override
    public String getReplyString() {
        return getReason();
    }
}
