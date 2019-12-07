package com.flysoohigh.IgniteCacheLoader.ignite;

import com.flysoohigh.IgniteCacheLoader.streaming.example.StreamVisitorExample;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
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
    private static final String CACHE_NAME = "TOKEN6_ZPAN";

    Logger logger = LoggerFactory.getLogger(IgniteCacheLoader.class);

    @PostConstruct
    public void main() {
        Ignition.setClientMode(true);

//        try (Ignite ignite = Ignition.start("/home/summer/dev/IGNITE/apache-ignite-2.7.6-bin/examples/config/example-ignite.xml")) {
        try (Ignite ignite = Ignition.start("/home/summer/dev/IGNITE/apache-ignite-2.7.6-bin/config/default-config.xml")) {

            CacheConfiguration<String, ImmutablePair<String, Integer>> cacheConfig = new CacheConfiguration<>(CACHE_NAME);
            cacheConfig.setIndexedTypes(String.class, ImmutablePair.class);

            try (IgniteCache<String, ImmutablePair<String, Integer>> instCache = ignite.getOrCreateCache(cacheConfig)){
                String cacheName = instCache.getName();
                try (IgniteDataStreamer<String, ImmutablePair<String, Integer>> streamer = ignite.dataStreamer(cacheName)){
                    logger.info("Start loading {} cache", cacheName);
                    StopWatch stopWatch = new StopWatch();
                    stopWatch.start();
//                    ClientCache<String, ImmutablePair<String, Integer>> myCache = igniteClient.getOrCreateCache(cacheName);
                    String line;

                    try(BufferedReader br = new BufferedReader(new FileReader("/home/summer/dev/JAVA/IgniteCacheLoader/token-data_4.csv"))) {
                        while ((line = br.readLine()) != null) {
                            String[] chunks = line.split(";");
                            String shardId = chunks[0];
                            String jibber = chunks[1];
                            String dekId = chunks[2].trim();
                            streamer.addData(shardId, new ImmutablePair<>(jibber, Integer.valueOf(dekId)));
                        }
                        logger.info("End loading {} cache", cacheName);
                        stopWatch.stop();
                        String seconds = new DecimalFormat("#.##").format(stopWatch.getTotalTimeSeconds());
                        logger.info("Total time loading {} cache is {} sec", cacheName, seconds);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            } finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
//                ignite.destroyCache(cacheConfig.getName());
            }

        }

//        ClientConfiguration cfg = new ClientConfiguration();
//        cfg.setAddresses("127.0.0.1:10800");
//        IgniteClient igniteClient = Ignition.startClient(cfg);

//        csvFileLocations.forEach(fileLocation -> loadCache(igniteClient, fileLocation));
    }

    private void loadCache(IgniteClient igniteClient, String csvFileLocation) {
        String cacheName = extractcacheName(csvFileLocation);
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

    private static String extractcacheName(String CSV_FILE_LOCATION) {
        Matcher matcher = Pattern.compile(REGEX).matcher(CSV_FILE_LOCATION);
        String cacheName = "";
        while (matcher.find()) {
            cacheName = matcher.group(1);
        }
        return cacheName;
    }
}
