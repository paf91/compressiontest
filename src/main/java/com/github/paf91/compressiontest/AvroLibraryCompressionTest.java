package com.github.paf91.compressiontest;

import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.xerial.snappy.SnappyOutputStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

public class AvroLibraryCompressionTest {

    private static final String LIBRARY_SCHEMA_STR =
            "{\n" +
                    "  \"type\":\"record\",\n" +
                    "  \"name\":\"Library\",\n" +
                    "  \"namespace\":\"com.github.paf91\",\n" +
                    "  \"fields\":[\n" +
                    "    { \"name\":\"address\", \"type\":\"string\"},\n" +
                    "    { \"name\":\"books\",\n" +
                    "      \"type\":{\n" +
                    "        \"type\":\"array\",\n" +
                    "        \"items\": {\n" +
                    "          \"type\":\"record\", \"name\":\"Book\",\n" +
                    "          \"fields\":[\n" +
                    "             {\"name\":\"title\",\"type\":[\"null\",\"string\"],\"default\":null},\n" +
                    "             {\"name\":\"pages\",\"type\":[\"null\",\"long\"],\"default\":null},\n" +
                    "             {\"name\":\"available\",\"type\":[\"null\",\"boolean\"],\"default\":null},\n" +
                    "             {\"name\":\"author\",\"type\":[\"null\",{\n" +
                    "                 \"type\":\"record\",\"name\":\"Author\",\n" +
                    "                 \"fields\":[\n" +
                    "                    {\"name\":\"name\",        \"type\":[\"null\",\"string\"],\"default\":null},\n" +
                    "                    {\"name\":\"surname\",     \"type\":[\"null\",\"string\"],\"default\":null},\n" +
                    "                    {\"name\":\"nationality\", \"type\":[\"null\",\"string\"],\"default\":null}\n" +
                    "                 ]\n" +
                    "             }],\"default\":null}\n" +
                    "          ]\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }\n" +
                    "  ]\n" +
                    "}";

    enum CompressionType {
        NONE, DEFLATE, SNAPPY
    }

    public static void main(String[] args) throws Exception {
        int numLibraries = 5;
        int booksPerLib = 20;
        List<DataGenerator.Library> data = DataGenerator.generateData(numLibraries, booksPerLib);

        Schema librarySchema = new Schema.Parser().parse(LIBRARY_SCHEMA_STR);

        long start = System.currentTimeMillis();
        GenericData.Array<GenericRecord> libraryRecords = toAvroLibraries(data, librarySchema);
        long end = System.currentTimeMillis();
        System.out.printf("Avro: building GenericRecords took %d ms, total=%d libraries%n",
                (end - start), data.size());

        writeAvroBinary(librarySchema, libraryRecords, "libraries.avro", CompressionType.NONE, "NoCompression");
        writeAvroBinary(librarySchema, libraryRecords, "libraries.avro.deflate", CompressionType.DEFLATE, "Deflate");
        writeAvroBinary(librarySchema, libraryRecords, "libraries.avro.snappy", CompressionType.SNAPPY, "Snappy");

        System.out.println("DONE AVRO TEST");
    }

    private static GenericData.Array<GenericRecord> toAvroLibraries(
            List<DataGenerator.Library> libs, Schema librarySchema) {

        GenericData.Array<GenericRecord> records = new GenericData.Array<>(libs.size(),
                Schema.createArray(librarySchema));

        for (DataGenerator.Library lib : libs) {
            GenericRecord libRec = new GenericData.Record(librarySchema);
            libRec.put("address", lib.address);

            Schema booksFieldSchema = librarySchema.getField("books").schema();
            Schema bookItemSchema = booksFieldSchema.getElementType();

            GenericData.Array<GenericRecord> bookArray =
                    new GenericData.Array<>(lib.books.size(), booksFieldSchema);

            for (DataGenerator.Book b : lib.books) {
                GenericRecord bookRec = new GenericData.Record(bookItemSchema);
                bookRec.put("title", b.title);
                bookRec.put("pages", b.pages);
                bookRec.put("available", b.available);

                if (b.author != null) {
                    // For union: [null, Author] choose the second branch (index 1)
                    Schema authorUnion = bookItemSchema.getField("author").schema();
                    Schema authorRecordSchema = authorUnion.getTypes().get(1);

                    GenericRecord authRec = new GenericData.Record(authorRecordSchema);
                    authRec.put("name", b.author.name);
                    authRec.put("surname", b.author.surname);
                    authRec.put("nationality", b.author.nationality);

                    bookRec.put("author", authRec);
                } else {
                    bookRec.put("author", null);
                }

                bookArray.add(bookRec);
            }

            libRec.put("books", bookArray);
            records.add(libRec);
        }
        return records;
    }

    /**
     * Writes Avro records directly in binary format without the container header.
     * If you use schema registry you might need to add 1 + 4 = 5 bytes to it
     * Supports wrapping the output stream with different compression codecs.
     *
     * @param librarySchema The Avro schema.
     * @param libs          The Avro records to write.
     * @param filename      The output filename.
     * @param compression   The desired compression type.
     * @param label         A label for logging purposes.
     * @throws Exception If an error occurs during writing.
     */
    private static void writeAvroBinary(Schema librarySchema,
                                        GenericData.Array<GenericRecord> libs,
                                        String filename,
                                        CompressionType compression,
                                        String label) throws Exception {
        long start = System.currentTimeMillis();
        File file = new File(filename);
        FileOutputStream fileOut = new FileOutputStream(file);
        OutputStream out;

        switch (compression) {
            case DEFLATE:
                out = new DeflaterOutputStream(fileOut, new Deflater(6));
                break;
            case SNAPPY:
                out = new SnappyOutputStream(fileOut);
                break;
            default:
                out = fileOut;
                break;
        }

        try {
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(librarySchema);
            // Create a binary encoder that writes directly to the output stream (no header)
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
}
