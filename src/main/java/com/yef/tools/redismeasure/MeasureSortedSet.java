package com.yef.tools.redismeasure;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MeasureSortedSet {

    private static final String redisHost = "localhost";
    private static final Integer redisPort = 6379;

    private static JedisPool pool = null;

    public MeasureSortedSet() {
        pool = new JedisPool(redisHost, redisPort);
    }

    public static void main(String[] args) {
        MeasureSortedSet main = new MeasureSortedSet();
        main.measureAll();
        pool.close();
    }

    public void measureAll() {
        Jedis jedis = pool.getResource();
        try {
            singleSortedSet(jedis);
            multipleSortedSet(jedis);
        } catch (JedisException e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    private void singleSortedSet(Jedis jedis) {
        int num = 100000;
        String key = "/ab/1234567890123456789";
        Long time = System.currentTimeMillis();

        jedis.flushAll();
        long before = extractUsedMemory(jedis.info("memory"));
        long timeBefore = System.currentTimeMillis();
        for (long i = 0; i < num; i++) {
            jedis.zadd(key, time++, String.valueOf(123456789012345L + i));
        }
        long after = extractUsedMemory(jedis.info("memory"));
        long timeAfter = System.currentTimeMillis();
        System.out.println("-----------------------");
        System.out.println("Used memory before: " + before + " bytes");
        System.out.println("Count of members in key '" + key + "': " + jedis.zcard(key));
        System.out.println("Used memory after: " + after + " bytes");
        System.out.println("Used memory difference: " + (after - before) + " bytes");
        System.out.println("Used memory per record: " + (after - before) / num + " bytes");
        System.out.println("Time per record: " + ((double) (timeAfter - timeBefore)) / num + " ms");
        System.out.println("-----------------------");
    }

    private void multipleSortedSet(Jedis jedis) {
        int num = 1000;
        int numKeys = 1000;
        long baseKey = 123456789012345678L;
        String key = "/ab/";
        Long time = System.currentTimeMillis();

        jedis.flushAll();
        long before = extractUsedMemory(jedis.info("memory"));
        long timeBefore = System.currentTimeMillis();
        for (int j = 0; j < numKeys; j++) {
            for (long i = 0; i < num; i++) {
                jedis.zadd(key + (baseKey + j), time++, String.valueOf(123456789012345L + i));
            }
        }
        long after = extractUsedMemory(jedis.info("memory"));
        long timeAfter = System.currentTimeMillis();
        System.out.println("-----------------------");
        System.out.println("Used memory before: " + before + " bytes");
        System.out.println("Count of members in all inserted keys: " + numKeys * num);
        System.out.println("Used memory after: " + after + " bytes");
        System.out.println("Used memory difference: " + (after - before) + " bytes");
        System.out.println("Used memory per record: " + (after - before) / (numKeys * num) + " bytes");
        System.out.println("Time per record: " + ((double) (timeAfter - timeBefore)) / (numKeys * num) + " ms");
        System.out.println("-----------------------");
    }

    private long extractUsedMemory(String src) {
        long ret = 0l;
        Pattern pattern = Pattern.compile("used_memory:([0-9]*)");
        Matcher matcher = pattern.matcher(src);
        if (matcher.find()) {
            ret = Long.parseLong(matcher.group(1));
        }
        return ret;
    }
}
