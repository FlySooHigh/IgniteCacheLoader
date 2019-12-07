package com.flysoohigh.IgniteCacheLoader.util;

import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.stream.IntStream;

public class CsvFileGenerator {
    // TODO: 07.12.2019 На 100_000_000 вылетает ООМ
    // Exception in thread "main" java.lang.OutOfMemoryError: Java heap space
    // at java.util.Arrays.copyOf(Arrays.java:3210)
    private static final int NUMBER_OF_LINES = 10_000_000;

    public static void main(String[] args) throws IOException {
        ArrayList<String> csvEntries = new ArrayList<>();
        System.out.println("Start generating csv file...");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
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
        stopWatch.stop();
        String seconds = new DecimalFormat("#.##").format(stopWatch.getTotalTimeSeconds());
        System.out.println("End generating csv file... total time " + seconds + " sec");
    }
}
