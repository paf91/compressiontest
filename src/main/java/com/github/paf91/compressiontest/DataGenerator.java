package com.github.paf91.compressiontest;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataGenerator {

    public static class Author {
        public String name;
        public String surname;
        public String nationality;
    }

    public static class Book {
        public String title;
        public Long pages;
        public Boolean available;
        public Author author;
    }

    public static class Library {
        public String address;
        public List<Book> books;
    }

    private static String randomString(Random rand, int length) {
        String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(alphabet.charAt(rand.nextInt(alphabet.length())));
        }
        return sb.toString();
    }

    public static List<Library> generateData(int numLibraries, int booksPerLibrary) {
        // Выведем в консоль, чтоб понимали масштаб
        System.out.println("Generating " + numLibraries + " libraries, "
                + booksPerLibrary + " books each...");

        Random rand = new Random(12345);
        List<Library> libraries = new ArrayList<>(numLibraries);

        for (int i = 0; i < numLibraries; i++) {
            Library lib = new Library();
            lib.address = "Street_" + randomString(rand, 5) + "_" + i;
            lib.books = new ArrayList<>(booksPerLibrary);

            for (int j = 0; j < booksPerLibrary; j++) {
                Book b = new Book();
                b.title = "Title_" + randomString(rand, 8) + "_" + j;
                b.pages = (long) (50 + rand.nextInt(500));
                b.available = rand.nextBoolean();

                // author
                if (rand.nextBoolean()) {
                    Author author = new Author();
                    author.name = "Name_" + randomString(rand, 5);
                    author.surname = "Surname_" + randomString(rand, 5);
                    if (rand.nextInt(3) < 2) {
                        author.nationality = "Nation_" + randomString(rand, 4);
                    }
                    b.author = author;
                } else {
                    b.author = null;
                }

                lib.books.add(b);
            }
            libraries.add(lib);
        }
        return libraries;
    }
}
