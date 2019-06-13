package org.tikv.raw;

import com.flipkart.lois.channel.api.Channel;
import com.flipkart.lois.channel.exceptions.ChannelClosedException;
import com.flipkart.lois.channel.impl.BufferedChannel;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;

import java.util.List;
import java.util.Random;

public class Main {
  private static final String PD_ADDRESS = "demo-pd-0.demo-pd-peer.tidb.svc:2379";
  private static final int DOCUMENT_SIZE = 1 << 10;
  private static final int NUM_COLLECTIONS = 1000_000;
  private static final int NUM_DOCUMENTS = 1000_000;
  private static final int NUM_READERS = 8;
  private static final int NUM_WRITERS = 8;
  private static final Logger logger = LoggerFactory.getLogger("Main");


  private static final String PROJECT_ID = "golden-path-tutorial-6522";
  private static final String INSTANCE_ID = "fuyang-test";


  private static List<Row> scan(BigtableDataClient client, String collection) {
    Query query = Query.create("test")
        .range(collection, null)
        .limit(100);

    return Lists.newArrayList(client.readRows(query));
  }

  private static void put(BigtableDataClient client, String collection, String key, String value) {

    RowMutation mutation = RowMutation.create("test", String.format("%s#%s", collection, key));
    mutation.setCell("c1", "q1", value);

    client.mutateRow(mutation);
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

  public static void main(String[] args) throws Exception {

    BigtableTableAdminClient tableAdminClient = BigtableTableAdminClient.create(PROJECT_ID, INSTANCE_ID);

    try {
      tableAdminClient.createTable(
          CreateTableRequest.of("test")
              .addFamily("c1")
      );
    } catch (Exception ignored) {
      logger.info("Table already exist");
    }
    finally {
      tableAdminClient.close();
    }
    BigtableDataClient client = BigtableDataClient.create(PROJECT_ID, INSTANCE_ID);

    Channel<Long> readTimes = new BufferedChannel<>(NUM_READERS * 10);
    Channel<Long> writeTimes = new BufferedChannel<>(NUM_WRITERS * 10);

    Channel<ReadAction> readActions = new BufferedChannel<>(NUM_READERS * 10);
    Channel<WriteAction> writeActions = new BufferedChannel<>(NUM_WRITERS * 10);

    new Thread(() -> {
      Random rand = new Random(System.nanoTime());
      while (true) {
        try {
          readActions.send(new ReadAction(String.format("collection-%d", rand.nextInt(NUM_COLLECTIONS))));
        } catch (InterruptedException e) {
          logger.warn("ReadAction Interrupted");
          return;
        } catch (ChannelClosedException e) {
          logger.warn("Channel has closed");
          return;
        }
      }
    }).start();

    new Thread(() -> {
      Random rand = new Random(System.nanoTime());
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


    for (int i = 0; i < NUM_WRITERS; i++) {
      runWrite(client, writeActions, writeTimes);
    }

    for (int i = 0; i < NUM_READERS; i++) {
      runRead(client, readActions, readTimes);
    }

    analyze("R", readTimes);
    analyze("W", writeTimes);

    System.out.println("Hello World!");
    while (true) ;
  }

  private static void resolve(Channel<Long> timings, long start) {
    try {
      timings.send(System.nanoTime() - start);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.out.println("Current thread interrupted. Test fail.");
    } catch (ChannelClosedException e) {
      logger.warn("Channel has closed");
    }
  }

  private static void runWrite(BigtableDataClient client, Channel<WriteAction> action, Channel<Long> timings) {
    new Thread(() -> {
      WriteAction writeAction;
      try {
        while ((writeAction = action.receive()) != null) {
          long start = System.nanoTime();
          put(client, writeAction.collection, writeAction.key, writeAction.value);
          resolve(timings, start);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        System.out.println("Current thread interrupted. Test fail.");
      } catch (ChannelClosedException e) {
        logger.warn("Channel has closed");
      }
    }).start();
  }

  private static void runRead(BigtableDataClient client, Channel<ReadAction> action, Channel<Long> timings) {
    new Thread(() -> {
      ReadAction readAction;
      try {
        while ((readAction = action.receive()) != null) {
          long start = System.nanoTime();
          scan(client, readAction.collection);
          resolve(timings, start);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        System.out.println("Current thread interrupted. Test fail.");
      } catch (ChannelClosedException e) {
        logger.warn("Channel has closed");
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

  private static void analyze(String label, Channel<Long> queue) {
    new Thread(() -> {
      long start = System.currentTimeMillis(), end;
      long total = 0;
      int count = 0;
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
