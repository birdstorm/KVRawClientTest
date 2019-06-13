package org.tikv.raw

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.produce

fun log(msg: String) = println("[${Thread.currentThread().name}] $msg")

fun main() = runBlocking {

    val numbers = numbersFrom(2)

    val primes = Channel<Int>(Channel.UNLIMITED)

    repeat(16) { finder(numbers, primes) }

    printer(primes).join()
}

fun CoroutineScope.numbersFrom(start: Int) = produce<Int>(capacity = 100) {
    var x = start
    while (true) send(x++) // infinite stream of integers from start
}

// Try remove Dispatchers.Default to see all jobs only run a single thread. Then it is concurrency, not parallelism
fun CoroutineScope.finder(numbers: ReceiveChannel<Int>, primes: SendChannel<Int>) = launch(Dispatchers.Default) {
    outer@ for (x in numbers) {
        for (i in 2 until x) {
            if (x % i == 0) continue@outer
        }
        log("found $x")
        primes.send(x) // is prime
    }
}

fun CoroutineScope.printer(numbers: ReceiveChannel<Int>) = launch {
    for (n in numbers) {
        log("$n")
    }
}