package com.github.robtimus.filesystems.ftp;

import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPFileFilter;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.util.ArrayList;
import java.util.List;

final class NonUnixFilesystemStrategy extends AbstractFTPFileStrategy {

    @Override
    public List<FTPFile> getChildren(FTPFileSystemClient FtpFileSystemClient, FTPPath path) throws IOException {

        FTPFile[] ftpFiles = FtpFileSystemClient.listFiles(path.path());

        boolean isDirectory = false;
        List<FTPFile> children = new ArrayList<>(ftpFiles.length);
        for (FTPFile ftpFile : ftpFiles) {
            String fileName = FTPFileSystem.getFileName(ftpFile);
            if (FTPFileSystem.CURRENT_DIR.equals(fileName)) {
                isDirectory = true;
            } else if (!FTPFileSystem.PARENT_DIR.equals(fileName)) {
                children.add(ftpFile);
            }
        }

        if (!isDirectory && children.size() <= 1) {
            // either zero or one, check the parent to see if the path exists and is a directory
            FTPPath currentPath = path;
            FTPFile currentFtpFile = getFTPFile(FtpFileSystemClient, currentPath);
            while (currentFtpFile.isSymbolicLink()) {
                currentPath = path.resolve(currentFtpFile.getLink());
                currentFtpFile = getFTPFile(FtpFileSystemClient, currentPath);
            }
            if (!currentFtpFile.isDirectory()) {
                throw new NotDirectoryException(path.path());
            }
        }

        return children;
    }

    @Override
    public FTPFile getFTPFile(FTPFileSystemClient FtpFileSystemClient, FTPPath path) throws IOException {
        final String parentPath = path.toAbsolutePath().parentPath();
        final String name = path.fileName();

        if (parentPath == null) {
            // path is /, but that cannot be listed
            FTPFile rootFtpFile = new FTPFile();
            rootFtpFile.setName("/"); //$NON-NLS-1$
            rootFtpFile.setType(FTPFile.DIRECTORY_TYPE);
            return rootFtpFile;
        }

        FTPFile[] ftpFiles = FtpFileSystemClient.listFiles(parentPath, new FTPFileFilter() {
            @Override
            public boolean accept(FTPFile ftpFile) {
                return name.equals(FTPFileSystem.getFileName(ftpFile));
            }
        });
        if (ftpFiles.length == 0) {
            throw new NoSuchFileException(path.path());
        }
        if (ftpFiles.length == 1) {
            return ftpFiles[0];
        }
        throw new IllegalStateException();
    }

    @Override
    public FTPFile getLink(FTPFileSystemClient FtpFileSystemClient, FTPFile ftpFile, FTPPath path) throws IOException {
        // getFTPFile always returns the entry in the parent, so there's no need to list the parent here.
        return ftpFile.getLink() == null ? null : ftpFile;
    }
}
