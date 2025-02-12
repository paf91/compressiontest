package com.github.paf91.compressiontest;

import com.github.paf91.compressiontest.DataGenerator.Author;
import com.github.paf91.compressiontest.DataGenerator.Book;
import com.github.paf91.compressiontest.DataGenerator.Library;
import org.xerial.snappy.Snappy;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class ProtobufLibraryCompressionTest {

    public static void main(String[] args) throws Exception {
        int numLibraries = 5;
        int booksPerLib = 20;
        List<Library> data = DataGenerator.generateData(numLibraries, booksPerLib);

        long start = System.currentTimeMillis();
        LibraryOuterClass.Libraries.Builder libsBuilder = LibraryOuterClass.Libraries.newBuilder();

        for (Library l : data) {
            LibraryOuterClass.Library.Builder libB = LibraryOuterClass.Library.newBuilder();
            libB.setAddress(l.address != null ? l.address : "");
            for (Book b : l.books) {
                LibraryOuterClass.Book.Builder bB = LibraryOuterClass.Book.newBuilder();
                if (b.title != null) bB.setTitle(b.title);
                if (b.pages != null) bB.setPages(b.pages);
                if (b.available != null) bB.setAvailable(b.available);

                if (b.author != null) {
                    Author a = b.author;
                    LibraryOuterClass.Author.Builder aB = LibraryOuterClass.Author.newBuilder();
                    if (a.name != null) aB.setName(a.name);
                    if (a.surname != null) aB.setSurname(a.surname);
                    if (a.nationality != null) aB.setNationality(a.nationality);
                    bB.setAuthor(aB.build());
                }
                libB.addBooks(bB.build());
            }
            libsBuilder.addLibraryList(libB.build());
        }
        LibraryOuterClass.Libraries libs = libsBuilder.build();
        long end = System.currentTimeMillis();
        System.out.printf("Protobuf building took %d ms, total libs= %d%n",
                (end - start), numLibraries);

        start = System.currentTimeMillis();
        byte[] rawBytes = libs.toByteArray();
        end = System.currentTimeMillis();
        System.out.printf("Proto raw: serialization took %d ms, size= %d bytes (%.2f KB)%n",
                (end - start), rawBytes.length, rawBytes.length / 1024.0);

        File protoFile = new File("libraries.protobuf");
        try (FileOutputStream fos = new FileOutputStream(protoFile)) {
            fos.write(rawBytes);
        }
        System.out.printf("No compression file= %s size= %d%n",
                protoFile.getName(), protoFile.length());

        File gzFile = new File("libraries.protobuf.gz");
        start = System.currentTimeMillis();
        try (FileOutputStream fos = new FileOutputStream(gzFile);
             GZIPOutputStream gzos = new GZIPOutputStream(fos)) {
            gzos.write(rawBytes);
        }
        end = System.currentTimeMillis();
        long gzSz = gzFile.length();
        System.out.printf("Proto GZIP: took %d ms, size= %d bytes (%.2f KB)%n",
                (end - start), gzSz, gzSz / 1024.0);

        File snappyFile = new File("libraries.protobuf.snappy");
        start = System.currentTimeMillis();
        byte[] snappyBytes = Snappy.compress(rawBytes);
        try (FileOutputStream fos = new FileOutputStream(snappyFile)) {
            fos.write(snappyBytes);
        }
        end = System.currentTimeMillis();
        long snappySz = snappyFile.length();
        System.out.printf("Proto Snappy: took %d ms, size= %d bytes (%.2f KB)%n",
                (end - start), snappySz, snappySz / 1024.0);

        System.out.println("DONE PROTOBUF TEST");
    }
}
