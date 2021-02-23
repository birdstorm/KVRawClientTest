package org.tikv.raw;

import com.flipkart.lois.channel.api.Channel;
import com.flipkart.lois.channel.exceptions.ChannelClosedException;
import com.flipkart.lois.channel.impl.BufferedChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb.KvPair;
import shade.com.google.protobuf.ByteString;

public class Main {
  private static final String PD_ADDRESS = "127.0.0.1:2379";
  private static final int DOCUMENT_SIZE = 10;
  private static final int NUM_COLLECTIONS = 100000000;
  private static final int NUM_DOCUMENTS = 100;
  private static final int NUM_READERS = 1;
  private static final int NUM_WRITERS = 32;
  private static final int NUM_READ_GENS = 32;
  private static final int NUM_WRITE_GENS = 4;
  private static final long PUT_TTL = 60;
  private static final Clock CLOCK = Clock.CLOCK;
  private static final Logger logger = Logger.getLogger("Main");

  private static ByteString get(RawKVClient client, String collection) {
    return client.get(ByteString.copyFromUtf8(collection));
  }

  private static void put(
      Map<ByteString, ByteString> values, String collection, String key, String value) {
    values.put(
        ByteString.copyFromUtf8(String.format("%s#%s", collection, key)),
        ByteString.copyFromUtf8(value));
  }

  private static void batchPut(RawKVClient client, Map<ByteString, ByteString> values, long ttl) {
    client.batchPut(values, ttl);
  }

  private static List<KvPair> scan(RawKVClient client, String collection, int limit) {
    return client.scan(ByteString.copyFromUtf8(collection), limit);
  }

  private static List<KvPair> scan(RawKVClient client, String start, String end, int limit) {
    return client.scan(ByteString.copyFromUtf8(start), ByteString.copyFromUtf8(end), limit);
  }

  private static List<KvPair> scanPrefix(RawKVClient client, String prefix, int limit) {
    ByteString start = ByteString.copyFromUtf8(prefix);
    ByteString end = ByteString.copyFrom(nextValue(prefix.getBytes()));
    return client.scan(start, end, limit);
  }

  private static byte[] nextValue(byte[] value) {
    byte[] newVal = Arrays.copyOf(value, value.length);

    int i;
    for (i = newVal.length - 1; i >= 0; --i) {
      ++newVal[i];
      if (newVal[i] != 0) {
        break;
      }
    }

    return i == -1 ? Arrays.copyOf(value, value.length + 1) : newVal;
  }

  private static class ReadAction {
    String collection;

    ReadAction(String collection) {
      this.collection = collection;
    }
  }

  private static class WriteAction {
    Map<ByteString, ByteString> values;
    long ttl;

    WriteAction(Map<ByteString, ByteString> values, long ttl) {
      this.values = values;
      this.ttl = ttl;
    }
  }

  public static void main(String[] args) {

    String log4jConfPath = "src/main/resources/log4j.properties";
    PropertyConfigurator.configure(log4jConfPath);

    Channel<Long> readTimes = new BufferedChannel<>(NUM_READERS * 10);
    Channel<Long> writeTimes = new BufferedChannel<>(NUM_WRITERS * 10);

    Channel<ReadAction> readActions = new BufferedChannel<>(NUM_READERS * 10);
    Channel<WriteAction> writeActions = new BufferedChannel<>(NUM_WRITERS * 10);

    for (int i = 0; i < NUM_READ_GENS; i++) {
      int index = i;
      new Thread(
              () -> {
                Random rand = new Random(CLOCK.now() + index);
                while (true) {
                  try {
                    if (readActions.isSendable()) {
                      readActions.send(
                          new ReadAction(
                              String.format("collection-%d", rand.nextInt(NUM_COLLECTIONS))));
                    }
                  } catch (InterruptedException e) {
                    logger.warn("ReadAction Interrupted");
                    return;
                  } catch (ChannelClosedException e) {
                    logger.warn("Channel has closed");
                    return;
                  }
                }
              })
          .start();
    }

    for (int i = 0; i < NUM_WRITE_GENS; i++) {
      int index = i;
      new Thread(
              () -> {
                Random rand = new Random(CLOCK.now() + index);
                while (true) {
                  try {
                    if (writeActions.isSendable()) {
                      Map<ByteString, ByteString> values = new HashMap<>();
                      for (int s = 0; s < 100; s++) {
                        put(
                            values,
                            String.format("collection-%d", rand.nextInt(NUM_COLLECTIONS)),
                            String.format("%d", rand.nextInt(NUM_DOCUMENTS)),
                            makeTerm(rand, DOCUMENT_SIZE));
                      }
                      writeActions.send(new WriteAction(values, PUT_TTL));
                    }
                  } catch (InterruptedException e) {
                    logger.warn("WriteAction Interrupted");
                    return;
                  } catch (ChannelClosedException e) {
                    logger.warn("Channel has closed");
                    return;
                  }
                }
              })
          .start();
    }

    for (int i = 0; i < NUM_READERS; i++) {
      try {
        TiSession session = TiSession.create(TiConfiguration.createRawDefault(PD_ADDRESS));
        runRead(session, readActions, readTimes);
      } catch (Exception e) {
        logger.fatal("error connecting to kv store: ", e);
      }
    }

    for (int i = 0; i < NUM_WRITERS; i++) {
      try {
        TiSession session = TiSession.create(TiConfiguration.createRawDefault(PD_ADDRESS));
        runWrite(session, writeActions, writeTimes);
      } catch (Exception e) {
        logger.fatal("error connecting to kv store: ", e);
      }
    }
    analyze("R", readTimes);
    analyze("W", writeTimes);

    logger.info("Test Start");
    while (true) ;
  }

  private static void resolve(Channel<Long> timings, long start) {
    try {
      timings.send(CLOCK.now() - start);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.info("Current thread interrupted. Test fail.");
    } catch (ChannelClosedException e) {
      logger.warn("Channel has closed");
    }
  }

  private static void runRead(
      TiSession session, Channel<ReadAction> action, Channel<Long> timings) {
    new Thread(
            () -> {
              try (RawKVClient client = session.createRawClient()) {
                ReadAction readAction;
                try {
                  while ((readAction = action.receive()) != null) {
                    long start = CLOCK.now();
                    ByteString bs = get(client, readAction.collection);
                    resolve(timings, start);
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  logger.info("Current thread interrupted. Test fail.");
                } catch (ChannelClosedException e) {
                  logger.warn("Channel has closed");
                }
              }
            })
        .start();
  }

  private static void runWrite(
      TiSession session, Channel<WriteAction> action, Channel<Long> timings) {
    new Thread(
            () -> {
              try (RawKVClient client = session.createRawClient()) {
                WriteAction writeAction;
                System.out.println("here comes a client!");
                try {
                  while ((writeAction = action.receive()) != null) {
                    long start = CLOCK.now();
                    batchPut(client, writeAction.values, writeAction.ttl);
                    resolve(timings, start);
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  logger.info("Current thread interrupted. Test fail.");
                } catch (ChannelClosedException e) {
                  logger.warn("Channel has closed");
                }
              }
            })
        .start();
  }

  private static final char[] LETTER_BYTES =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

  private static String makeTerm(Random rand, int n) {
    char[] b = new char[n];
    for (int i = 0; i < n; i++) {
      b[i] = LETTER_BYTES[rand.nextInt(LETTER_BYTES.length)];
    }
    return String.valueOf(b);
  }

  private static void analyze(String label, Channel<Long> queue) {
    new Thread(
            () -> {
              long start = CLOCK.now() / 1000, end;
              long total = 0, sum = 0;
              int count = 0, sumCount = 0;
              logger.info("start label " + label);
              while (true) {
                try {
                  total += queue.receive() / 1000;
                  count++;
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  logger.info("Current thread interrupted. Test fail.");
                } catch (ChannelClosedException e) {
                  logger.warn("Channel has closed");
                  return;
                }
                end = CLOCK.now() / 1000;
                if (end - start > 1000000) {
                  sum += total;
                  sumCount += count;
                  logger.info(
                      String.format(
                          "[%s] % 6d total updates, avg = % 9d us, tot avg = % 9d us\n",
                          label, count, total / count, sum / sumCount));
                  total = 0;
                  count = 0;
                  start = end;
                }
              }
            })
        .start();
  }
}
