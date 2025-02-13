package com.github.paf91.compressiontest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.xerial.snappy.SnappyOutputStream;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class JsonLibraryCompressionTestLineByLine {

    public static void main(String[] args) throws Exception {
        int numLibraries = 1000;
        int booksPerLibrary = 20;

        // Используем дата-классы из DataGenerator
        List<DataGenerator.Library> libraries = DataGenerator.generateData(numLibraries, booksPerLibrary);
        System.out.println("Total Libraries = " + libraries.size());

        // Массив вариантов компрессии: "null" (без компрессии), "gzip" и "snappy"
        String[] compressions = {"null", "gzip", "snappy"};

        for (String compression : compressions) {
            runTest(libraries, compression, "JsonLibraryCompressionTestLineByLine-" + compression);
        }
    }

    private static void runTest(List<DataGenerator.Library> libraries, String compression, String label) throws Exception {
        File file = new File(label + ".json");
        FileOutputStream fos = new FileOutputStream(file);
        OutputStream out;

        if ("gzip".equalsIgnoreCase(compression)) {
            out = new GZIPOutputStream(fos);
        } else if ("snappy".equalsIgnoreCase(compression)) {
            out = new SnappyOutputStream(fos);
        } else {
            out = fos;
        }

        ObjectMapper mapper = new ObjectMapper();
        long start = System.currentTimeMillis();

        // Записываем весь список библиотек как единый JSON (как в старом коде)
        String jsonString = mapper.writeValueAsString(libraries);
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"))) {
            writer.write(jsonString);
            writer.flush();
        }
        long end = System.currentTimeMillis();
        long sz = file.length();
        System.out.printf("[%s] Written JSON file: %s, size=%d bytes (%.2f KB), took %d ms%n",
                label, file.getName(), sz, sz / 1024.0, (end - start));
    }
}
