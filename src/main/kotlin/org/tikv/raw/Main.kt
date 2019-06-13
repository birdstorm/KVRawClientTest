package org.tikv.raw

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.tikv.common.TiConfiguration
import org.tikv.common.TiSession
import shade.com.google.protobuf.ByteString
import java.util.*

private val logger = KotlinLogging.logger {}

private val PD_ADDRESS = "demo-pd-0.demo-pd-peer.tidb.svc:2379"
private val DOCUMENT_SIZE = 1 shl 10
private val NUM_COLLECTIONS = 1000_000
private val NUM_DOCUMENTS = 1000_000
private val NUM_READERS = 64
private val NUM_WRITERS = 64

val conf = TiConfiguration.createRawDefault(PD_ADDRESS)
val session = TiSession.create(conf)

data class ReadAction(val collection: String)
data class WriteAction(val collection: String, val key: String, val value: String)

fun main() = runBlocking {

    val readTimes = Channel<Long>(Channel.UNLIMITED) // unbuffered channel to store all the reading time in nano sec
    val writeTimes = Channel<Long>(Channel.UNLIMITED) // unbuffered channel to store all the writing time in nano sec

    val readActions = produce<ReadAction>(Dispatchers.Default, capacity = NUM_READERS * 100) {
        val rand = Random(System.nanoTime())
        while (true) {
            logger.debug("produce read action ...")
            send(ReadAction(String.format("collection-%d", rand.nextInt(NUM_COLLECTIONS))))
        }
    }
    val writeActions = produce<WriteAction>(Dispatchers.Default, capacity = NUM_WRITERS * 100) {
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
        val tiClient = session.createRawClient()
        launchReader(tiClient, readActions, readTimes)
    }
    repeat(NUM_WRITERS) {
        val tiClient = session.createRawClient()
        launchWriter(tiClient, writeActions, writeTimes)
    }

    analyzeTiming("Read", readTimes)
    analyzeTiming("Write", writeTimes).join()

//    logger.info("Test started...")
}

fun CoroutineScope.launchReader(
        tiClient: RawKVClient,
        channel: ReceiveChannel<ReadAction>,
        timingChannel: SendChannel<Long>) = launch(Dispatchers.IO) {
    for (readAction in channel) {
        val start = System.nanoTime()
        logger.debug { "scan collection: $readAction.collection" }
        try {
            tiClient.scan(ByteString.copyFromUtf8(readAction.collection), 100)
        } catch (exeption: Exception) {
            logger.warn { "Scan failed. ${exeption.message}" }
        }
        timingChannel.send(System.nanoTime() - start) // store reading time in nano sec
    }
}

fun CoroutineScope.launchWriter(
        tiClient: RawKVClient,
        channel: ReceiveChannel<WriteAction>,
        timingChannel: SendChannel<Long>) = launch(Dispatchers.IO) {
    for (writeAction in channel) {
        logger.debug { "put key: $writeAction.collection#$writeAction.key" }
        val start = System.nanoTime()
        try {
            tiClient.put(ByteString.copyFromUtf8("$writeAction.collection#$writeAction.key"),
                    ByteString.copyFromUtf8(writeAction.value))
        } catch (exeption: Exception) {
            logger.warn { "Put failed. ${exeption.message}" }
        }
        timingChannel.send(System.nanoTime() - start) // store reading time in nano sec
    }
}

fun CoroutineScope.analyzeTiming(label: String, channel: ReceiveChannel<Long>) = launch {
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

fun Long.d(digits: Int) = java.lang.String.format("%${digits}d", this)

private val LETTER_BYTES = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray()
private fun makeTerm(rand: Random, n: Int): String {
    val b = CharArray(n)
    for (i in 0 until n) {
        b[i] = LETTER_BYTES[rand.nextInt(LETTER_BYTES.size)]
    }
    return String(b)
}