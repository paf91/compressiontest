package com.github.paf91.compressiontest;

import com.github.paf91.compressiontest.DataGenerator.Library;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class JsonLibraryCompressionTest {

    public static void main(String[] args) throws IOException {

        int numLibraries = 5;
        int booksPerLibrary = 20;

        List<Library> data = DataGenerator.generateData(numLibraries, booksPerLibrary);
        System.out.println("Total Libraries = " + data.size());

        ObjectMapper mapper = new ObjectMapper();

        long start = System.currentTimeMillis();
        String jsonString = mapper.writeValueAsString(data);
        long end = System.currentTimeMillis();
        long noCompressionBytes = jsonString.getBytes().length;

        System.out.printf("JSON raw: serialization took %d ms, size= %d bytes (%.2f KB)%n",
                (end - start), noCompressionBytes, noCompressionBytes / 1024.0);

        // Raw
        File noCompressFile = new File("libraries.json");
        try (FileWriter fw = new FileWriter(noCompressFile)) {
            fw.write(jsonString);
        }
        System.out.println("No compression file: " + noCompressFile.getAbsolutePath()
                + ", size=" + noCompressFile.length());

        // GZIP
        File gzipFile = new File("libraries.json.gz");
        start = System.currentTimeMillis();
        try (FileOutputStream fos = new FileOutputStream(gzipFile);
             GZIPOutputStream gzos = new GZIPOutputStream(fos);
             OutputStreamWriter osw = new OutputStreamWriter(gzos)) {
            osw.write(jsonString);
        }
        end = System.currentTimeMillis();
        long gzBytes = gzipFile.length();
        System.out.printf("JSON gzip: writing took %d ms, size= %d bytes (%.2f KB)%n",
                (end - start), gzBytes, gzBytes / 1024.0);

        // Snappy
        File snappyFile = new File("libraries.json.snappy");
        start = System.currentTimeMillis();
        byte[] compressed = Snappy.compress(jsonString.getBytes());
        try (FileOutputStream fos = new FileOutputStream(snappyFile)) {
            fos.write(compressed);
        }
        end = System.currentTimeMillis();
        long snappySize = snappyFile.length();
        System.out.printf("JSON snappy: writing took %d ms, size= %d bytes (%.2f KB)%n",
                (end - start), snappySize, snappySize / 1024.0);

        System.out.println("DONE JSON TEST");
    }
}
