package org.livescore;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.io.FilenameUtils;

import java.io.*;

public class Walker {

    private final Parser parser;

    public Walker(Parser parser) {
        this.parser = parser;
    }

    public void walk(String fileOrFolder) throws IOException, ArchiveException {
        File file = new File(fileOrFolder);
        walk(file);
    }

    public void walk(File file) throws IOException, ArchiveException {
        if (!file.exists())
            return;

        if (file.isDirectory()) {
            File[] filesList = file.listFiles();
            if (filesList == null)
                return;

            for (File innerFile : filesList) {
                walk(innerFile);
            }
        } else {
            String extension = FilenameUtils.getExtension(file.getName());
            if ("log".equals(extension)) {
                parser.parse(file);
                return;
            }

            if ("7z".equals(extension)) {
                walk7z(file);
                return;
            }

            if ("bz2".equals(extension)) {
                walkBz2(file);
            }
        }
    }

    private void walk7z(File file) throws IOException, ArchiveException {
        System.out.println("Processing " + file.getName());

        try(SevenZFile sevenZFile = new SevenZFile(file)) {
            SevenZArchiveEntry entry;
            while ((entry = sevenZFile.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }

                byte[] content = new byte[(int) entry.getSize()];
                sevenZFile.read(content, 0, (int) entry.getSize());

                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(content);
                walkStream(byteArrayInputStream, entry.getName());
                byteArrayInputStream.close();
            }
        }
    }

    private void walkStream(InputStream inputStream, String name) throws IOException, ArchiveException {
        System.out.println("Processing " + name);

        String extension = FilenameUtils.getExtension(name);
        if ("log".equals(extension)) {
            parser.parse(inputStream);
            return;
        }

        if ("bz2".equals(extension)) {
            walkBz2(inputStream);
        }
    }

    private void walkBz2(File file) throws IOException, ArchiveException {
        InputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file));
        try (ArchiveInputStream input = new ArchiveStreamFactory()
                .createArchiveInputStream(bufferedInputStream)) {
            ArchiveEntry entry;
            while ((entry = input.getNextEntry()) != null) {
                if (!input.canReadEntryData(entry)) {
                    continue;
                }

                if (entry.isDirectory()) {
                    continue;
                }

                byte[] content = new byte[(int) entry.getSize()];
                input.read(content, 0, (int) entry.getSize());
            }
        } catch (Exception e) {
            int i = 0;
        }
    }

    private void walkBz2(InputStream inputStream) throws IOException {
        BZip2CompressorInputStream input = new BZip2CompressorInputStream(inputStream);
        byte[] content = input.readAllBytes();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(content);
        parser.parse(byteArrayInputStream);
        byteArrayInputStream.close();
        input.close();
    }

}
