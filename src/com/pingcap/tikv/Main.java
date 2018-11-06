
package com.pingcap.tikv;

import com.pingcap.tikv.kvproto.Kvrpcpb;
import org.apache.log4j.Logger;
import shade.com.google.protobuf.ByteString;

import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

public class Main {
  private static final String PD_ADDRESS = "127.0.0.1:2379";
  private static final int DOCUMENT_SIZE = 1 << 10;
  private static final int NUM_COLLECTIONS = 10;
  private static final int NUM_DOCUMENTS = 100;
  private static final int NUM_READERS = 1;
  private static final int NUM_WRITERS = 32;

  private static List<Kvrpcpb.KvPair> scan(KVRawClient client, String collection) {
    return client.scan(ByteString.copyFromUtf8(collection), 100);
  }

  private static void put(KVRawClient client, String collection, String key, String value) {
    client.put(ByteString.copyFromUtf8(String.format("%s#%s", collection, key)), ByteString.copyFromUtf8(value));
  }

  private static class ReadAction {
    String collection;

    ReadAction(String collection) {
      this.collection = collection;
    }
  }

  private static class WriteAction {
    String collection;
    String key;
    String value;

    WriteAction(String collection, String key, String value) {
      this.collection = collection;
      this.key = key;
      this.value = value;
    }
  }

  public static void main(String[] args) {
    Logger logger = Logger.getLogger("Main");

    BlockingQueue<Long> readTimes = new LinkedBlockingDeque<>(NUM_READERS * 10);
    BlockingQueue<Long> writeTimes = new LinkedBlockingDeque<>(NUM_WRITERS * 10);

    BlockingQueue<ReadAction> readActions = new LinkedBlockingDeque<>(NUM_READERS * 10);
    BlockingQueue<WriteAction> writeActions = new LinkedBlockingDeque<>(NUM_WRITERS * 10);

    new Thread(() -> {
      Random rand = new Random(System.nanoTime());
      while (true) {
        try {
          readActions.put(new ReadAction(String.format("collection-%d", rand.nextInt(NUM_COLLECTIONS))));
        } catch (InterruptedException e) {
          logger.warn("ReadAction Interrupted");
          return;
        }
      }
    }).start();

    new Thread(() -> {
      Random rand = new Random(System.nanoTime());
      while (true) {
        try {
          writeActions.put(new WriteAction(String.format("collection-%d", rand.nextInt(NUM_COLLECTIONS)), String.format("%d", rand.nextInt(NUM_DOCUMENTS)), makeTerm(rand, DOCUMENT_SIZE)));
        } catch (InterruptedException e) {
          logger.warn("ReadAction Interrupted");
          return;
        }
      }
    }).start();


    for (int i = 0; i < NUM_WRITERS; i ++) {
      KVRawClient client;
      try {
        client = KVRawClient.create(PD_ADDRESS);
      } catch (Exception e) {
        logger.fatal("error connecting to kv store: ", e);
        continue;
      }
      runWrite(client, writeActions, writeTimes);
    }

    for (int i = 0; i < NUM_READERS; i ++) {
      KVRawClient client;
      try {
        client = KVRawClient.create(PD_ADDRESS);
      } catch (Exception e) {
        logger.fatal("error connecting to kv store: ", e);
        continue;
      }
      runRead(client, readActions, readTimes);
    }

    analyze("R", readTimes);
    analyze("W", writeTimes);

    System.out.println("Hello World!");
    while(true);
  }

  private static void runWrite(KVRawClient client, BlockingQueue<WriteAction> action, BlockingQueue<Long> timings) {
    new Thread(() -> {
      WriteAction writeAction;
      while ((writeAction = action.poll()) != null) {
        long start = System.nanoTime();
        put(client, writeAction.collection, writeAction.key, writeAction.value);
        try {
          timings.put(System.nanoTime() - start);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          System.out.println("Current thread interrupted. Test fail.");
        }
      }
    }).start();
  }

  private static void runRead(KVRawClient client, BlockingQueue<ReadAction> action, BlockingQueue<Long> timings) {
    new Thread(() -> {
      ReadAction readAction;
      while ((readAction = action.poll()) != null) {
        long start = System.nanoTime();
        scan(client, readAction.collection);
        try {
          timings.put(System.nanoTime() - start);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          System.out.println("Current thread interrupted. Test fail.");
        }
      }
    }).start();
  }

  private static final char[] LETTER_BYTES = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

  private static String makeTerm(Random rand, int n) {
    char[] b = new char[n];
    for (int i = 0; i < n; i++) {
      b[i] = LETTER_BYTES[rand.nextInt(LETTER_BYTES.length)];
    }
    return String.valueOf(b);
  }

  private static void analyze(String label, BlockingQueue<Long> queue) {
    new Thread(() -> {
      long start = System.currentTimeMillis(), end;
      long total = 0;
      int count = 0;
      System.out.println("start label " + label);
      while (true) {
        try {
          total += queue.take() / 1000;
          count++;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          System.out.println("Current thread interrupted. Test fail.");
        }
        end = System.currentTimeMillis();
        if (end - start > 1000) {
          System.out.println(String.format("[%s] % 6d total updates, avg = % 9d us\n", label, count, total / count));
          total = 0;
          count = 0;
          start = end;
        }
      }
    }).start();
  }
}
