/*
 * FTPClientPool.java
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

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPFileFilter;

/**
 * A pool of FTP clients, allowing multiple commands to be executed concurrently.
 *
 * @author Rob Spoor
 */
final class FTPClientPool {

    private final Logger LOG = Logger.getLogger(FTPClientPool.class.getName());

    private final String hostname;
    private final int port;

    private final FTPEnvironment env;
    private final FileSystemExceptionFactory exceptionFactory;

    private final BlockingQueue<Client> pool;

    FTPClientPool(String hostname, int port, FTPEnvironment env) throws IOException {
        this.hostname = hostname;
        this.port = port;
        this.env = env.clone();
        this.exceptionFactory = env.getExceptionFactory();
        final int poolSize = env.getClientConnectionCount();
        this.pool = new ArrayBlockingQueue<>(poolSize);

        try {
            for (int i = 0; i < poolSize; i++) {
                pool.add(new Client(true));
            }
        } catch (IOException e) {
            // creating the pool failed, disconnect all clients
            for (Client client : pool) {
                try {
                    client.disconnect();
                } catch (IOException e2) {
                    e.addSuppressed(e2);
                }
            }
            throw e;
        }
    }

    Client get() throws IOException {
        try {
            Client client = pool.take();
            try {
                if (!client.isConnected()) {
                    client = new Client(true);
                }
            } catch (final Exception e) {
                // could not create a new client; re-add the broken client to the pool to prevent pool starvation
                pool.add(client);
                throw e;
            }
            client.increaseRefCount();
            return client;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            InterruptedIOException iioe = new InterruptedIOException(e.getMessage());
            iioe.initCause(e);
            throw iioe;
        }
    }

    Client getOrCreate() throws IOException {
        Client client = pool.poll();
        if (client == null) {
            // nothing was taken from the pool, so no risk of pool starvation if creating the client fails
            return new Client(false);
        }
        try {
            if (!client.isConnected()) {
                client = new Client(true);
            }
        } catch (final Exception e) {
            // could not create a new client; re-add the broken client to the pool to prevent pool starvation
            pool.add(client);
            throw e;
        }
        client.increaseRefCount();
        return client;
    }

    void keepAlive() throws IOException {
        List<Client> clients = new ArrayList<>();
        pool.drainTo(clients);

        IOException exception = null;
        for (Client client : clients) {
            try {
                client.keepAlive();
            } catch (IOException e) {
                exception = add(exception, e);
            } finally {
                returnToPool(client);
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    boolean isSecure() {
        return env instanceof FTPSEnvironment;
    }

    void close() throws IOException {
        List<Client> clients = new ArrayList<>();
        pool.drainTo(clients);

        IOException exception = null;
        for (Client client : clients) {
            try {
                client.disconnect();
            } catch (IOException e) {
                exception = add(exception, e);
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    private IOException add(IOException existing, IOException e) {
        if (existing == null) {
            return e;
        }
        existing.addSuppressed(e);
        return existing;
    }

    private void returnToPool(Client client) {
        assert client.refCount == 0;

        pool.add(client);
    }

    final class Client implements FTPFileSystemClient {

        public static final int KEEP_ALIVE_MIN_INTERVAL = 5000;
        private final FTPClient client;
        private final boolean pooled;

        private FileType fileType;
        private FileStructure fileStructure;
        private FileTransferMode fileTransferMode;

        private int refCount = 0;
        private long lastPacketTimeMs;

        private Client(boolean pooled) throws IOException {
            this.client = env.createClient(hostname, port);
            this.pooled = pooled;

            this.fileType = env.getDefaultFileType();
            this.fileStructure = env.getDefaultFileStructure();
            this.fileTransferMode = env.getDefaultFileTransferMode();
        }

        private void increaseRefCount() {
            refCount++;
        }

        private int decreaseRefCount() {
            if (refCount > 0) {
                refCount--;
            }
            return refCount;
        }

        @Override
        public void keepAlive() throws IOException {
            if (env.ftpClientDebug()) {
                this.debug("keepAlive");
            }
            this.lastPacketTimeMs = System.currentTimeMillis();
            client.sendNoOp();
        }

        private boolean isConnected() {
            if (client.isConnected()) {
                return checkKeepAlive();
            }
            return false;
        }

        private boolean checkKeepAlive() {
            long now = System.currentTimeMillis();
            if (lastPacketTimeMs < now - KEEP_ALIVE_MIN_INTERVAL) {
                try {
                    keepAlive();
                    return true;
                } catch (@SuppressWarnings("unused") IOException e) {
                    // the keep alive failed - treat as not connected, and actually disconnect quietly
                    disconnectQuietly();
                    return false;
                }
            } else {
                return true;
            }
        }

        private void disconnect() throws IOException {
            client.disconnect();
        }

        private void disconnectQuietly() {
            try {
                client.disconnect();
            } catch (@SuppressWarnings("unused") IOException e) {
                // ignore
            }
        }

        @Override
        public void close() throws IOException {
            if (decreaseRefCount() == 0) {
                if (pooled) {
                    returnToPool(this);
                } else {
                    disconnect();
                }
            }
        }

        @Override
        public String pwd() throws IOException {
            if (env.ftpClientDebug()) {
                this.debug("pwd");
            }
            this.lastPacketTimeMs = System.currentTimeMillis();

            String pwd = client.printWorkingDirectory();
            if (pwd == null) {
                throw new FTPFileSystemException(client.getReplyCode(), client.getReplyString());
            }
            return pwd;
        }

        private void applyTransferOptions(TransferOptions options) throws IOException {
            if (options.fileType != null && options.fileType != fileType) {
                options.fileType.apply(client);
                fileType = options.fileType;
            }
            if (options.fileStructure != null && options.fileStructure != fileStructure) {
                options.fileStructure.apply(client);
                fileStructure = options.fileStructure;
            }
            if (options.fileTransferMode != null && options.fileTransferMode != fileTransferMode) {
                options.fileTransferMode.apply(client);
                fileTransferMode = options.fileTransferMode;
            }
        }

        @Override
        @SuppressWarnings("resource")
        public InputStream newInputStream(String path, OpenOptions options) throws IOException {
            assert options.read;
            if (env.ftpClientDebug()) {
                this.debug("newInputStream", path);
            }
            this.lastPacketTimeMs = System.currentTimeMillis();

            applyTransferOptions(options);

            InputStream in = client.retrieveFileStream(path);
            if (in == null) {
                throw exceptionFactory.createNewInputStreamException(path, client.getReplyCode(), client.getReplyString());
            }
            refCount++;
            return new FTPInputStream(path, in, options.deleteOnClose);
        }

        private final class FTPInputStream extends InputStream {

            private final String path;
            private final InputStream in;
            private final boolean deleteOnClose;

            private boolean open = true;

            private FTPInputStream(String path, InputStream in, boolean deleteOnClose) {
                this.path = path;
                this.in = in;
                this.deleteOnClose = deleteOnClose;
            }

            @Override
            public int read() throws IOException {
                return in.read();
            }

            @Override
            public int read(byte[] b) throws IOException {
                return in.read(b);
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return in.read(b, off, len);
            }

            @Override
            public long skip(long n) throws IOException {
                return in.skip(n);
            }

            @Override
            public int available() throws IOException {
                return in.available();
            }

            @Override
            public void close() throws IOException {
                if (open) {
                    in.close();
                    open = false;
                    finalizeStream();
                    if (deleteOnClose) {
                        delete(path, false);
                    }
                }
            }

            @Override
            public synchronized void mark(int readlimit) {
                in.mark(readlimit);
            }

            @Override
            public synchronized void reset() throws IOException {
                in.reset();
            }

            @Override
            public boolean markSupported() {
                return in.markSupported();
            }
        }

        @Override
        @SuppressWarnings("resource")
        public OutputStream newOutputStream(String path, OpenOptions options) throws IOException {
            assert options.write;
            if (env.ftpClientDebug()) {
                this.debug("newOutputStream", path);
            }
            this.lastPacketTimeMs = System.currentTimeMillis();


            applyTransferOptions(options);

            OutputStream out = options.append ? client.appendFileStream(path) : client.storeFileStream(path);
            if (out == null) {
                throw exceptionFactory.createNewOutputStreamException(path, client.getReplyCode(), client.getReplyString(), options.options);
            }
            refCount++;
            return new FTPOutputStream(path, out, options.deleteOnClose);
        }

        private final class FTPOutputStream extends OutputStream {

            private final String path;
            private final OutputStream out;
            private final boolean deleteOnClose;

            private boolean open = true;

            private FTPOutputStream(String path, OutputStream out, boolean deleteOnClose) {
                this.path = path;
                this.out = out;
                this.deleteOnClose = deleteOnClose;
            }

            @Override
            public void write(int b) throws IOException {
                out.write(b);
            }

            @Override
            public void write(byte[] b) throws IOException {
                out.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                out.write(b, off, len);
            }

            @Override
            public void flush() throws IOException {
                out.flush();
            }

            @Override
            public void close() throws IOException {
                if (open) {
                    out.close();
                    open = false;
                    finalizeStream();
                    if (deleteOnClose) {
                        delete(path, false);
                    }
                }
            }
        }

        private void finalizeStream() throws IOException {
            assert refCount > 0;

            if (!client.completePendingCommand()) {
                throw new FTPFileSystemException(client.getReplyCode(), client.getReplyString());
            }
            if (decreaseRefCount() == 0) {
                if (pooled) {
                    returnToPool(Client.this);
                } else {
                    disconnect();
                }
            }
        }

        @Override
        public void storeFile(String path, InputStream local, TransferOptions options, Collection<? extends OpenOption> openOptions) throws IOException {
            applyTransferOptions(options);

            this.lastPacketTimeMs = System.currentTimeMillis();
            if (!client.storeFile(path, local)) {
                throw exceptionFactory.createNewOutputStreamException(path, client.getReplyCode(), client.getReplyString(), openOptions);
            }
        }

        @Override
        public FTPFile[] listFiles(String path) throws IOException {
            if (env.ftpClientDebug()) {
                this.debug("list", path);
            }
            this.lastPacketTimeMs = System.currentTimeMillis();
            return client.listFiles(path);
        }

        @Override
        public FTPFile[] listFiles(String path, FTPFileFilter filter) throws IOException {
            if (env.ftpClientDebug()) {
                this.debug("list", path, filter);
            }
            this.lastPacketTimeMs = System.currentTimeMillis();
            return client.listFiles(path, filter);
        }

        @Override
        public void throwIfEmpty(String path, FTPFile[] ftpFiles) throws IOException {
            if (ftpFiles.length == 0) {
                throw exceptionFactory.createGetFileException(path, client.getReplyCode(), client.getReplyString());
            }
        }

        @Override
        public void mkdir(String path) throws IOException {
            if (env.ftpClientDebug()) {
                this.debug("mkdir", path);
            }
            this.lastPacketTimeMs = System.currentTimeMillis();
            if (!client.makeDirectory(path)) {
                throw exceptionFactory.createCreateDirectoryException(path, client.getReplyCode(), client.getReplyString());
            }
        }

        @Override
        public void delete(String path, boolean isDirectory) throws IOException {
            if (env.ftpClientDebug()) {
                this.debug("delete", path);
            }

            this.lastPacketTimeMs = System.currentTimeMillis();
            boolean success = isDirectory ? client.removeDirectory(path) : client.deleteFile(path);
            if (!success) {
                throw exceptionFactory.createDeleteException(path, client.getReplyCode(), client.getReplyString(), isDirectory);
            }
        }

        @Override
        public void rename(String source, String target) throws IOException {
            if (env.ftpClientDebug()) {
                this.debug("rename", source, target);
            }
            this.lastPacketTimeMs = System.currentTimeMillis();

            if (!client.rename(source, target)) {
                throw exceptionFactory.createMoveException(source, target, client.getReplyCode(), client.getReplyString());
            }
        }

        @Override
        public Calendar mdtm(String path) throws IOException {
            if (env.ftpClientDebug()) {
                this.debug("mdtm", path);
            }
            this.lastPacketTimeMs = System.currentTimeMillis();
            FTPFile file = client.mdtmFile(path);
            return file == null ? null : file.getTimestamp();
        }

        private void debug(String cmd, Object... args) {
            String argsString = Arrays.stream(args)
                    .map(a -> a.toString())
                    .map(s -> " " + s)
                    .reduce(String::concat)
                    .orElse("");
            LOG.log(Level.FINER, cmd + argsString);
        }

    }
}
