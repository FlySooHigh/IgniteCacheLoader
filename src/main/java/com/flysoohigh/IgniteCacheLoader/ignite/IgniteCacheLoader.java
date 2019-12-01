package com.flysoohigh.IgniteCacheLoader.ignite;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class IgniteCacheLoader {

    @Value("#{'${csv.file.locations}'.split(',')}")
    private List<String> csvFileLocations;

    private static final int TEMP_MAP_SIZE_TO_DUMP_INTO_CACHE = 1_000;
    private static final String REGEX = "/([-_a-zA-Z0-9]+)\\.csv";

    Logger logger = LoggerFactory.getLogger(IgniteCacheLoader.class);

    @PostConstruct
    public void main() {
        ClientConfiguration cfg = new ClientConfiguration();
        cfg.setAddresses("127.0.0.1:10800");
        IgniteClient igniteClient = Ignition.startClient(cfg);

        csvFileLocations.forEach(fileLocation -> loadCache(igniteClient, fileLocation));
    }

    private void loadCache(IgniteClient igniteClient, String csvFileLocation) {
        String cacheName = extractCacheName(csvFileLocation);
        logger.info("Start loading {} cache", cacheName);
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        ClientCache<String, ImmutablePair<String, Integer>> myCache = igniteClient.getOrCreateCache(cacheName);
        String line;
        Map<String, ImmutablePair<String, Integer>> tempMap = new HashMap<>();
        int counter = 0;

        try(BufferedReader br = new BufferedReader(new FileReader(csvFileLocation))) {
            while ((line = br.readLine()) != null) {
                String[] chunks = line.split(";");
                String shardId = chunks[0];
                String jibber = chunks[1];
                String dekId = chunks[2].trim();
                tempMap.put(shardId, new ImmutablePair<>(jibber, Integer.valueOf(dekId)));
                if (tempMap.size() == TEMP_MAP_SIZE_TO_DUMP_INTO_CACHE) {
                    myCache.putAll(tempMap);
                    tempMap = new HashMap<>();
//                    System.out.println(TEMP_MAP_SIZE_TO_DUMP_INTO_CACHE * ++counter);
                }
            }
            logger.info("End loading {} cache", cacheName);
            stopWatch.stop();
            String seconds = new DecimalFormat("#.##").format(stopWatch.getTotalTimeSeconds());
            logger.info("Total time loading {} cache is {} sec", cacheName, seconds);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String extractCacheName(String CSV_FILE_LOCATION) {
        Matcher matcher = Pattern.compile(REGEX).matcher(CSV_FILE_LOCATION);
        String cacheName = "";
        while (matcher.find()) {
            cacheName = matcher.group(1);
        }
        return cacheName;
    }
}
