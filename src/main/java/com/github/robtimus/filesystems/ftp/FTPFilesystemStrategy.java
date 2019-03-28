package com.github.robtimus.filesystems.ftp;

import org.apache.commons.net.ftp.FTPFile;

import java.io.IOException;
import java.util.List;

public interface FTPFilesystemStrategy {

    List<FTPFile> getChildren(FTPFileSystemClient FtpFileSystemClient, FTPPath path) throws IOException;

    FTPFile getFTPFile(FTPFileSystemClient FtpFileSystemClient, FTPPath path) throws IOException;

    FTPFile getLink(FTPFileSystemClient FtpFileSystemClient, FTPFile ftpFile, FTPPath path) throws IOException;
}
