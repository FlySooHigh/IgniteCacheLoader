package com.flysoohigh.IgniteCacheLoader.ignite;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class IgniteCacheLoader {

    @Value("#{'${csv.file.locations}'.split(',')}")
    private List<String> csvFileLocations;
    @Value("${ignite.config.path}")
    private String igniteConfigPath;

    private static final String REGEX = "/([-_a-zA-Z0-9]+)\\.csv";

    Logger logger = LoggerFactory.getLogger(IgniteCacheLoader.class);

    @PostConstruct
    public void main() {
        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start(igniteConfigPath)) {
            csvFileLocations.forEach(fileLocation -> loadStmCache(ignite, fileLocation));
        }
    }

    private void loadStmCache(Ignite ignite, String fileLocation) {

        String cacheName = extractcacheName(fileLocation);

        CacheConfiguration<String, ImmutablePair<String, Integer>> cacheConfig = new CacheConfiguration<>(cacheName);
        cacheConfig.setIndexedTypes(String.class, ImmutablePair.class);

        try (IgniteDataStreamer<String, ImmutablePair<String, Integer>> dataStreamer = ignite.dataStreamer(cacheName)){
            logger.info("Start loading {} cache", cacheName);
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            String line;

            try(BufferedReader br = new BufferedReader(new FileReader(fileLocation))) {
                while ((line = br.readLine()) != null) {
                    String[] chunks = line.split(";");
                    String shardId = chunks[0];
                    String jibber = chunks[1];
                    String dekId = chunks[2].trim();
                    dataStreamer.addData(shardId, new ImmutablePair<>(jibber, Integer.valueOf(dekId)));
                }
                logger.info("End loading {} cache", cacheName);
                stopWatch.stop();
                String seconds = new DecimalFormat("#.##").format(stopWatch.getTotalTimeSeconds());
                logger.info("Total time loading {} cache is {} sec", cacheName, seconds);
            } catch (IOException e) {
                e.printStackTrace();
            }
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
