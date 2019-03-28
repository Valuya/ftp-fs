/*
 * AbstractFTPFileStrategy.java
 * Copyright 2017 Rob Spoor
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

import java.io.IOException;

import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPFileFilter;

/**
 * A strategy for handling FTP files in an FTP server specific way. This will help support FTP servers that return the current directory (.) when
 * listing directories, and FTP servers that don't.
 *
 * @author Rob Spoor
 */
abstract class AbstractFTPFileStrategy implements FTPFilesystemStrategy {

    static NonUnixFilesystemStrategy NON_UNIX_INSTANCE;
    static UnixFilesystemStrategy UNIX_INSTANCE;

    static AbstractFTPFileStrategy getInstance(FTPFileSystemClient FtpFileSystemClient, boolean supportAbsoluteFilePaths) throws IOException {
        if (!supportAbsoluteFilePaths) {
            // NonUnix uses the parent directory to list files
            return AbstractFTPFileStrategy.NON_UNIX_INSTANCE;
        }

        FTPFile[] ftpFiles = FtpFileSystemClient.listFiles("/", new FTPFileFilter() { //$NON-NLS-1$
            @Override
            public boolean accept(FTPFile ftpFile) {
                String fileName = FTPFileSystem.getFileName(ftpFile);
                return FTPFileSystem.CURRENT_DIR.equals(fileName);
            }
        });
        return ftpFiles.length == 0 ? AbstractFTPFileStrategy.NON_UNIX_INSTANCE : UnixFilesystemStrategy.UNIX_INSTANCE;
    }

}
