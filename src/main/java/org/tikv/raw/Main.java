package org.tikv.raw;

import com.flipkart.lois.channel.api.Channel;
import com.flipkart.lois.channel.exceptions.ChannelClosedException;
import com.flipkart.lois.channel.impl.BufferedChannel;
import org.apache.log4j.PropertyConfigurator;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.apache.log4j.Logger;
import shade.com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Main {
  private static final String PD_ADDRESS = "127.0.0.1:2379";
  private static final int DOCUMENT_SIZE = 10;
  private static final int NUM_COLLECTIONS = 10;
  private static final int NUM_DOCUMENTS = 100;
  private static final int NUM_READERS = 1;
  private static final int NUM_WRITERS = 2;
  private static final int NUM_GENS = 5;
  private static final Clock CLOCK = Clock.CLOCK;
  private static final Logger logger = Logger.getLogger("Main");

//  private static List<Kvrpcpb.KvPair> scan(RawKVClient client, String collection) {
//    return client.scan(ByteString.copyFromUtf8(collection), 100);
//  }

  private static void put(Map<ByteString, ByteString> values, String collection, String key, String value) {
    values.put(ByteString.copyFromUtf8(String.format("%s#%s", collection, key)), ByteString.copyFromUtf8(value));
  }

  private static void batchPut(RawKVClient client, Map<ByteString, ByteString> values) {
    client.batchPut(values);
  }

//  private static class ReadAction {
//    String collection;
//
//    ReadAction(String collection) {
//      this.collection = collection;
//    }
//  }

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

//    String log4jConfPath = "./log4j.properties";
//    PropertyConfigurator.configure(log4jConfPath);

//    Channel<Long> readTimes = new BufferedChannel<>(NUM_READERS * 10);
    Channel<Long> writeTimes = new BufferedChannel<>(NUM_WRITERS * 10);

//    Channel<ReadAction> readActions = new BufferedChannel<>(NUM_READERS * 10);
    Channel<WriteAction> writeActions = new BufferedChannel<>(NUM_WRITERS * 10);

//    new Thread(() -> {
//      Random rand = new Random(System.nanoTime());
//      while (true) {
//        try {
//          readActions.send(new ReadAction(String.format("collection-%d", rand.nextInt(NUM_COLLECTIONS))));
//        } catch (InterruptedException e) {
//          logger.warn("ReadAction Interrupted");
//          return;
//        } catch (ChannelClosedException e) {
//          logger.warn("Channel has closed");
//          return;
//        }
//      }
//    }).start();

    for (int i = 0; i < NUM_GENS; i++) {
      int index = i;
      new Thread(() -> {
        Random rand = new Random(CLOCK.now() + index);
        while (true) {
          try {
            writeActions.send(new WriteAction(String.format("collection-%d", rand.nextInt(NUM_COLLECTIONS)), String.format("%d", rand.nextInt(NUM_DOCUMENTS)), makeTerm(rand, DOCUMENT_SIZE)));
          } catch (InterruptedException e) {
            logger.warn("WriteAction Interrupted");
            return;
          } catch (ChannelClosedException e) {
            logger.warn("Channel has closed");
            return;
          }
        }
      }).start();
    }

    for (int i = 0; i < NUM_WRITERS; i++) {
      try {
        TiSession session = TiSession.create(TiConfiguration.createRawDefault(PD_ADDRESS));
        RawKVClient client = session.createRawClient();
        runWrite(client, writeActions, writeTimes);
      } catch (Exception e) {
        logger.fatal("error connecting to kv store: ", e);
      }
    }

//    for (int i = 0; i < NUM_READERS; i++) {
//      RawKVClient client;
//      try {
//        client = RawKVClient.create(PD_ADDRESS);
//      } catch (Exception e) {
//        logger.fatal("error connecting to kv store: ", e);
//        continue;
//      }
//      runRead(client, readActions, readTimes);
//    }

//    analyze("R", readTimes);
    analyze("W", writeTimes);

    System.out.println("Test Start");
    while (true) ;
  }

  private static void resolve(Channel<Long> timings, long start) {
    try {
      timings.send(CLOCK.now() - start);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.out.println("Current thread interrupted. Test fail.");
    } catch (ChannelClosedException e) {
      logger.warn("Channel has closed");
    }
  }

  private static void runWrite(RawKVClient client, Channel<WriteAction> action, Channel<Long> timings) {
    new Thread(() -> {
      WriteAction writeAction;
      try {
        int s = 0, limit = 100;
        Map<ByteString, ByteString> values = new HashMap<>(limit);
        while ((writeAction = action.receive()) != null) {
          put(values, writeAction.collection, writeAction.key, writeAction.value);
          if (s >= limit) {
            long start = CLOCK.now();
            batchPut(client, values);
            resolve(timings, start);
            values.clear();
            s = 0;
          }
          ++s;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        System.out.println("Current thread interrupted. Test fail.");
      } catch (ChannelClosedException e) {
        logger.warn("Channel has closed");
      }
    }).start();
  }

//  private static void runRead(RawKVClient client, Channel<ReadAction> action, Channel<Long> timings) {
//    new Thread(() -> {
//      ReadAction readAction;
//      try {
//        while ((readAction = action.receive()) != null) {
//          long start = System.nanoTime();
//          scan(client, readAction.collection);
//          resolve(timings, start);
//        }
//      } catch (InterruptedException e) {
//        Thread.currentThread().interrupt();
//        System.out.println("Current thread interrupted. Test fail.");
//      } catch (ChannelClosedException e) {
//        logger.warn("Channel has closed");
//      }
//    }).start();
//  }

  private static final char[] LETTER_BYTES = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

  private static String makeTerm(Random rand, int n) {
    char[] b = new char[n];
    for (int i = 0; i < n; i++) {
      b[i] = LETTER_BYTES[rand.nextInt(LETTER_BYTES.length)];
    }
    return String.valueOf(b);
  }

  private static void analyze(String label, Channel<Long> queue) {
    new Thread(() -> {
      long start = CLOCK.now(), end;
      long total = 0, sum = 0;
      int count = 0, sumCount = 0;
      System.out.println("start label " + label);
      while (true) {
        try {
          total += queue.receive() / 1000;
          count++;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          System.out.println("Current thread interrupted. Test fail.");
        } catch (ChannelClosedException e) {
          logger.warn("Channel has closed");
          return;
        }
        end = CLOCK.now();
        if (end - start > 1000) {
          sum += total;
          sumCount += count;
          logger.info(String.format("[%s] % 6d total updates, avg = % 9d us, tot avg = % 9d us\n", label, count, total / count, sum / sumCount));
          total = 0;
          count = 0;
          start = end;
        }
      }
    }).start();
  }
}
