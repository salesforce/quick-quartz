/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import java.io.OutputStream

/**
 * prefer this for single threaded use because it does not synchronize any of its methods
 */
class YetAnotherFastByteArrayOutputStream(initialCapacity: Int = 1024) : OutputStream() {

    private var buff = ByteArray(initialCapacity)
    private var currSize = 0

    override fun write(b: ByteArray?) {
        b?.apply {
            ensureCapacity(currSize + b.size)
            System.arraycopy(b, 0, buff, currSize, b.size)
            currSize += b.size
        }
    }

    override fun write(b: ByteArray?, off: Int, len: Int) {
        b?.apply {
            ensureCapacity(currSize + len)
            System.arraycopy(b, off, buff, currSize, len)
            currSize += len
        }
    }

    /** Ensures that we have a large enough buffer for the given size.  */
    private fun ensureCapacity(newCapacity: Int) {
        if (newCapacity > buff.size) {
            val old = buff
            buff = ByteArray(Math.max(newCapacity, 2 * buff.size))
            System.arraycopy(old, 0, buff, 0, old.size)
        }
    }

    /**
     * returns a copy of the buffer trimmed to the correct position.
     */
    fun toByteArray(): ByteArray {
        val c = ByteArray(currSize)
        System.arraycopy(buff, 0, c, 0, currSize)
        return c
    }

    override fun write(b: Int) {
        TODO("not implemented")
    }
}
