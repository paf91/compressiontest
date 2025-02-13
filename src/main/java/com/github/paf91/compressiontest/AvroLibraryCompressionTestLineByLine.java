package com.github.paf91.compressiontest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DeflaterOutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.generic.GenericDatumWriter;
import org.xerial.snappy.SnappyOutputStream;

public class AvroLibraryCompressionTestLineByLine {

    private static final Schema BOOK_SCHEMA = getBookSchema();
    private static final Schema LIBRARY_SCHEMA = getLibrarySchema();

    public static void main(String[] args) throws Exception {
        int numLibraries = 1000;
        int booksPerLib = 20;

        List<GenericRecord> libs = createLibraries(numLibraries, booksPerLib);

        String[] compressions = {"null", "deflate", "snappy"};

        for (String compression : compressions) {
            runTest(libs, compression, "AvroLibraryCompressionTestLineByLine-" + compression);
        }
    }

    private static void runTest(List<GenericRecord> libs, String compression, String label) throws Exception {
        File file = new File(label + ".avro");
        FileOutputStream fos = new FileOutputStream(file);
        OutputStream out;
        if ("deflate".equalsIgnoreCase(compression)) {
            out = new DeflaterOutputStream(fos);
        } else if ("snappy".equalsIgnoreCase(compression)) {
             out = new SnappyOutputStream(fos);
        } else {
            out = fos;
        }

        long start = System.currentTimeMillis();
        try {
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(LIBRARY_SCHEMA);
            Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            for (GenericRecord record : libs) {
                datumWriter.write(record, encoder);
            }
            encoder.flush();
        } finally {
            out.close();
        }
        long end = System.currentTimeMillis();
        long sz = file.length();
        System.out.printf("[%s] Written binary Avro file: %s, size=%d bytes (%.2f KB), took %d ms%n",
                label, file.getName(), sz, sz / 1024.0, (end - start));
    }

    private static List<GenericRecord> createLibraries(int numLibraries, int booksPerLib) {
        List<GenericRecord> libs = new ArrayList<>();
        for (int i = 0; i < numLibraries; i++) {
            GenericRecord lib = new GenericData.Record(LIBRARY_SCHEMA);
            lib.put("name", "Library " + i);
            // Генерируем список книг для текущей библиотеки
            List<GenericRecord> books = new ArrayList<>();
            for (int j = 0; j < booksPerLib; j++) {
                GenericRecord book = new GenericData.Record(BOOK_SCHEMA);
                book.put("title", "Book " + j);
                book.put("author", "Author " + j);
                books.add(book);
            }
            lib.put("books", books);
            libs.add(lib);
        }
        return libs;
    }

    // Пример метода для получения схемы книги
    private static Schema getBookSchema() {
        String bookSchemaJson = "{"
                + "\"type\": \"record\","
                + "\"name\": \"Book\","
                + "\"fields\": ["
                + "  {\"name\": \"title\", \"type\": \"string\"},"
                + "  {\"name\": \"author\", \"type\": \"string\"}"
                + "]"
                + "}";
        return new Schema.Parser().parse(bookSchemaJson);
    }

    // Пример метода для получения схемы библиотеки
    private static Schema getLibrarySchema() {
        String librarySchemaJson = "{"
                + "\"type\": \"record\","
                + "\"name\": \"Library\","
                + "\"fields\": ["
                + "  {\"name\": \"name\", \"type\": \"string\"},"
                + "  {\"name\": \"books\", \"type\": {\"type\": \"array\", \"items\": " + getBookSchema().toString() + "}}"
                + "]"
                + "}";
        return new Schema.Parser().parse(librarySchemaJson);
    }
}
