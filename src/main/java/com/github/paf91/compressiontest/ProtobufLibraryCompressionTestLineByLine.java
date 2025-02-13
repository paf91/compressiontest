package com.github.paf91.compressiontest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.xerial.snappy.SnappyOutputStream;

import com.github.paf91.compressiontest.LibraryOuterClass.Author;
import com.github.paf91.compressiontest.LibraryOuterClass.Book;
import com.github.paf91.compressiontest.LibraryOuterClass.Library;

public class ProtobufLibraryCompressionTestLineByLine {
    public static void main(String[] args) throws Exception {
        int numLibraries = 1000;
        int booksPerLib = 20;

        List<Library> libraries = createLibraries(numLibraries, booksPerLib);

        String[] compressions = {"null", "gzip", "snappy"};

        for (String compression : compressions) {
            runTest(libraries, compression, "ProtobufLibraryCompressionTestLineByLine-" + compression);
        }
    }

    private static void runTest(List<Library> libraries, String compression, String label) throws Exception {
        File file = new File(label + ".pb");
        FileOutputStream fos = new FileOutputStream(file);
        OutputStream out;

        if ("gzip".equalsIgnoreCase(compression)) {
            out = new GZIPOutputStream(fos);
        } else if ("snappy".equalsIgnoreCase(compression)) {
            out = new SnappyOutputStream(fos);
        } else {
            out = fos;
        }

        long start = System.currentTimeMillis();
        for (Library lib : libraries) {
            lib.writeDelimitedTo(out);
        }
        out.close();
        long end = System.currentTimeMillis();
        long sz = file.length();
        System.out.printf("[%s] Written Protobuf file: %s, size=%d bytes (%.2f KB), took %d ms%n",
                label, file.getName(), sz, sz / 1024.0, (end - start));
    }

    private static List<Library> createLibraries(int numLibraries, int booksPerLib) {
        List<Library> libraries = new ArrayList<>();
        for (int i = 0; i < numLibraries; i++) {
            Library.Builder libBuilder = Library.newBuilder();
            libBuilder.setAddress("Library Address " + i);
            for (int j = 0; j < booksPerLib; j++) {
                Book.Builder bookBuilder = Book.newBuilder();
                bookBuilder.setTitle("Book " + j);
                bookBuilder.setPages(100 + j);
                bookBuilder.setAvailable(j % 2 == 0);

                Author.Builder authorBuilder = Author.newBuilder();
                authorBuilder.setName("AuthorName " + j);
                authorBuilder.setSurname("AuthorSurname " + j);
                authorBuilder.setNationality("Nationality " + j);

                bookBuilder.setAuthor(authorBuilder);
                libBuilder.addBooks(bookBuilder);
            }
            libraries.add(libBuilder.build());
        }
        return libraries;
    }
}
