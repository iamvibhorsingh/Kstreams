package com.learning.kafkastreaming.chapter4;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.Tuple;

import java.util.List;

public class RedisManager implements Runnable {

    /****************************************************************************
     * This Class prints leaderboards from redis server running
     * on localhost:6379.
     ***************************************************************************
     **/

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_BLUE = "\u001B[34m";

    private static final String lbKey = "player-leaderboard";

    private Jedis jedis;

    public static void main(String[] args) {
        RedisManager rmgr = new RedisManager();
        rmgr.setUp();
        Thread testThread = new Thread(rmgr);
        testThread.start();

        try (Jedis jedisWriter = new Jedis("localhost", 6379)) {
            jedisWriter.zincrby(lbKey, 2, "Mouse");
            jedisWriter.zincrby(lbKey, 3, "Keyboard");
            Thread.sleep(6000);
            jedisWriter.zincrby(lbKey, 1, "Monitor");
            jedisWriter.zincrby(lbKey, 2, "Mouse");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setUp() {
        try {
            jedis = new Jedis("localhost", 6379);
            jedis.del(lbKey);
            System.out.println("Redis connection setup successfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void update_score(String product, double count) {
        if (jedis != null) {
            jedis.zincrby(lbKey, count, product);
        }
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                if (jedis != null) {
                    List<Tuple> scores = jedis.zrevrangeWithScores(lbKey, 0, -1);
                    int position = 1;
                    for (Tuple score : scores) {
                        System.out.println(
                                ANSI_BLUE + "Leaderboard - " + position + " : "
                                        + score.getElement() + " = " + score.getScore()
                                        + ANSI_RESET);
                        position++;
                    }
                }
                Thread.sleep(5000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
