# Avro vs. JSON vs. Protobuf Compression Tests
This project aims to compare data formats in size and compression time

Generates N number of Library data structure with N number of Book entries, then serializes it in three formats:

1. **Avro** (`AvroLibraryCompressionTest`)
    - We write a single Avro record (with nested "Book" array, "Author") to different files:
        - No compression
        - Snappy
        - Deflate
    - Compare file sizes and time taken.

2. **JSON** (`JsonLibraryCompressionTest`)
    - We serialize the same structure to JSON (using Jackson).
    - Then write:
        - no-compression `.json`
        - gzip-compressed `.json.gz`
        - snappy-compressed `.json.snappy`
    - Compare sizes and performance.

3. **Protobuf** (`ProtobufLibraryCompressionTest`)
    - Using the same concept (library, books, author).
    - We generate a `Library` message, serialize to bytes, then store:
        - raw `.bin`
        - gzip
        - snappy
    - Compare file sizes and serialization speed.

## Prerequisites

- Java 11+
- Maven 3.6+ (or higher)

## How to build and run

You might want to compile your LibraryOuterClass using protoc

In windows:
```bash
protoc.exe --java_out=. .\library.proto
```
In linux however it's simply as:
```bash
protoc --java_out=. library.proto
```

Then compile and run:
```bash
mvn clean compile
# windows
mvn exec:java -D"exec.mainClass=com.github.paf91.compressiontest.JsonLibraryCompressionTest"
mvn exec:java -D"exec.mainClass=com.github.paf91.compressiontest.AvroLibraryCompressionTest"
mvn exec:java -D"exec.mainClass=com.github.paf91.compressiontest.ProtobufLibraryCompressionTest"
#linux
mvn exec:java -Dexec.mainClass="com.github.paf91.compressiontest.AvroLibraryCompressionTest"
mvn exec:java -Dexec.mainClass="com.github.paf91.compressiontest.JsonLibraryCompressionTest"
mvn exec:java -Dexec.mainClass="com.github.paf91.compressiontest.ProtobufLibraryCompressionTest"