package org.tikv.raw

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.models.Query
import com.google.cloud.bigtable.data.v2.models.Row
import com.google.cloud.bigtable.data.v2.models.RowMutation
import com.google.common.collect.Lists
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.tikv.raw.Constants.Companion.DOCUMENT_SIZE
import org.tikv.raw.Constants.Companion.NUM_COLLECTIONS
import org.tikv.raw.Constants.Companion.NUM_DOCUMENTS
import org.tikv.raw.Constants.Companion.NUM_READERS
import org.tikv.raw.Constants.Companion.NUM_WRITERS
import org.tikv.raw.Constants.Companion.SCAN_LIMIT
import java.util.*


private val logger = KotlinLogging.logger {}

private val PROJECT_ID = "golden-path-tutorial-6522"
private val INSTANCE_ID = "fuyang-test"

fun main() = runBlocking {

    val tableAdminClient = BigtableTableAdminClient.create(PROJECT_ID, INSTANCE_ID)
    try {
        tableAdminClient.createTable(
                CreateTableRequest.of("test")
                        .addFamily("c1")
        )
    } catch (ignored: Exception) {
        logger.info("Table already exist")
    } finally {
        tableAdminClient.close()
    }

    val client = BigtableDataClient.create(PROJECT_ID, INSTANCE_ID)

    val readTimes = Channel<Long>(Channel.UNLIMITED) // unbuffered channel to store all the reading time in nano sec
    val writeTimes = Channel<Long>(Channel.UNLIMITED) // unbuffered channel to store all the writing time in nano sec

    val readActions = produce<ReadAction>(Dispatchers.IO, capacity = NUM_READERS * 1000) {
        val rand = Random(System.nanoTime())
        while (true) {
            logger.debug("produce read action ...")
            send(ReadAction(String.format("collection-%d", rand.nextInt(NUM_COLLECTIONS))))
        }
    }
    val writeActions = produce<WriteAction>(Dispatchers.IO, capacity = NUM_WRITERS * 1000) {
        val rand = Random(System.nanoTime())
        while (true) {
            logger.debug("produce write action ...")
            send(WriteAction(
                    String.format("collection-%d", rand.nextInt(NUM_COLLECTIONS)),
                    String.format("%d", rand.nextInt(NUM_DOCUMENTS)),
                    makeTerm(rand, DOCUMENT_SIZE)))
        }
    }

    repeat(NUM_READERS) {
        launchReader(client, readActions, readTimes)
    }
    repeat(NUM_WRITERS) {
        launchWriter(client, writeActions, writeTimes)
    }

    analyzeTiming("Bigtable Read", readTimes)
    analyzeTiming("Bigtable Write", writeTimes).join()
}

fun CoroutineScope.launchReader(
        client: BigtableDataClient,
        channel: ReceiveChannel<ReadAction>,
        timingChannel: SendChannel<Long>) = launch(Dispatchers.IO) {
    for (readAction in channel) {
        val start = System.nanoTime()
        logger.debug { "scan collection: ${readAction.collection}" }
        try {
            val query = Query.create("test")
                    .range(readAction.collection, null)
                    .limit(SCAN_LIMIT.toLong())
            val newArrayList: ArrayList<Row> = Lists.newArrayList(client.readRows(query))
        } catch (exeption: Exception) {
            logger.warn { "Scan failed. ${exeption.message}" }
        }
        timingChannel.send(System.nanoTime() - start) // store reading time in nano sec
    }
}

fun CoroutineScope.launchWriter(
        client: BigtableDataClient,
        channel: ReceiveChannel<WriteAction>,
        timingChannel: SendChannel<Long>) = launch(Dispatchers.IO) {
    for (writeAction in channel) {
        logger.debug { "put key: ${writeAction.collection}#${writeAction.key}" }
        val start = System.nanoTime()
        try {
            val mutation = RowMutation.create("test", "${writeAction.collection}#${writeAction.key}")
            mutation.setCell("c1", "q1", writeAction.value)

            client.mutateRow(mutation)
        } catch (exeption: Exception) {
            logger.warn { "Put failed. ${exeption.message}" }
        }
        timingChannel.send(System.nanoTime() - start) // store reading time in nano sec
    }
}

private fun CoroutineScope.analyzeTiming(label: String, channel: ReceiveChannel<Long>) = launch {
    logger.info { "Start analyzing label $label" }
    var startMs = System.currentTimeMillis()
    var endMs: Long
    var totalMicroS = 0L
    var count = 0L
    while (true) {
        totalMicroS += channel.receive() / 1000 // nanoseconds to microseconds
        count++
        endMs = System.currentTimeMillis()
        if (endMs - startMs > 1000) { // print and clear state for every 1000 ms
            logger.info { "[$label] ${count.d(6)} total actions, avg time = ${(totalMicroS / count)} μs" }
            totalMicroS = 0
            count = 0
            startMs = endMs
        }
    }
}

private val LETTER_BYTES = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray()
private fun makeTerm(rand: Random, n: Int): String {
    val b = CharArray(n)
    for (i in 0 until n) {
        b[i] = LETTER_BYTES[rand.nextInt(LETTER_BYTES.size)]
    }
    return String(b)
}