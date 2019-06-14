package org.tikv.raw

data class ReadAction(val collection: String)
data class WriteAction(val collection: String, val key: String, val value: String)

class Constants {
    companion object {
        val SCAN_LIMIT = 100
        val DOCUMENT_SIZE = 1 shl 10
        val NUM_COLLECTIONS = 1000_000
        val NUM_DOCUMENTS = 1000_000
        val NUM_READERS = 0
        val NUM_WRITERS = 64
    }
}

fun Long.d(digits: Int) = java.lang.String.format("%${digits}d", this)