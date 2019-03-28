package com.github.robtimus.filesystems.ftp;

import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPFileFilter;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.util.ArrayList;
import java.util.List;

final class UnixFilesystemStrategy extends AbstractFTPFileStrategy {

    private static final FTPFilesystemStrategy INSTANCE = new UnixFilesystemStrategy();

    @Override
    public List<FTPFile> getChildren(FTPFileSystemClient FtpFileSystemClient, FTPPath path) throws IOException {

        FTPFile[] ftpFiles = FtpFileSystemClient.listFiles(path.path());

        if (ftpFiles.length == 0) {
            throw new NoSuchFileException(path.path());
        }
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

        if (!isDirectory) {
            throw new NotDirectoryException(path.path());
        }

        return children;
    }

    @Override
    public FTPFile getFTPFile(FTPFileSystemClient FtpFileSystemClient, FTPPath path) throws IOException {
        final String name = path.fileName();

        FTPFile[] ftpFiles = FtpFileSystemClient.listFiles(path.path(), new FTPFileFilter() {
            @Override
            public boolean accept(FTPFile ftpFile) {
                String fileName = FTPFileSystem.getFileName(ftpFile);
                return FTPFileSystem.CURRENT_DIR.equals(fileName) || (name != null && name.equals(fileName));
            }
        });
        FtpFileSystemClient.throwIfEmpty(path.path(), ftpFiles);
        if (ftpFiles.length == 1) {
            return ftpFiles[0];
        }
        for (FTPFile ftpFile : ftpFiles) {
            if (FTPFileSystem.CURRENT_DIR.equals(FTPFileSystem.getFileName(ftpFile))) {
                return ftpFile;
            }
        }
        throw new IllegalStateException();
    }

    @Override
    public FTPFile getLink(FTPFileSystemClient FtpFileSystemClient, FTPFile ftpFile, FTPPath path) throws IOException {
        if (ftpFile.getLink() != null) {
            return ftpFile;
        }
        if (ftpFile.isDirectory() && FTPFileSystem.CURRENT_DIR.equals(FTPFileSystem.getFileName(ftpFile))) {
            // The file is returned using getFTPFile, which returns the . (current directory) entry for directories.
            // List the parent (if any) instead.

            final String parentPath = path.toAbsolutePath().parentPath();
            final String name = path.fileName();

            if (parentPath == null) {
                // path is /, there is no link
                return null;
            }

            FTPFile[] ftpFiles = FtpFileSystemClient.listFiles(parentPath, new FTPFileFilter() {
                @Override
                public boolean accept(FTPFile ftpFile) {
                    return (ftpFile.isDirectory() || ftpFile.isSymbolicLink()) && name.equals(FTPFileSystem.getFileName(ftpFile));
                }
            });
            FtpFileSystemClient.throwIfEmpty(path.path(), ftpFiles);
            return ftpFiles[0].getLink() == null ? null : ftpFiles[0];
        }
        return null;
    }
}
