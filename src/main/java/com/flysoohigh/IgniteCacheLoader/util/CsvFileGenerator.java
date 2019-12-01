package com.flysoohigh.IgniteCacheLoader.util;

import org.apache.commons.lang3.RandomStringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.stream.IntStream;

public class CsvFileGenerator {
    private static final int NUMBER_OF_LINES = 10_000_000;

    public static void main(String[] args) throws IOException {
        ArrayList<String> csvEntries = new ArrayList<>();
        System.out.println("Start generating csv file...");
        IntStream.rangeClosed(1, NUMBER_OF_LINES).forEach(num -> {
            String csvEntry = new StringBuilder()
                    .append(num)
                    .append(";")
                    .append(RandomStringUtils.randomAlphabetic(10))
                    .append(";")
                    .append(RandomStringUtils.randomNumeric(3))
                    .append(" ")
                    .toString();
            csvEntries.add(csvEntry);
        });
        Files.write(Paths.get("data.csv"), csvEntries);
        System.out.println("End generating csv file...");
    }
}
