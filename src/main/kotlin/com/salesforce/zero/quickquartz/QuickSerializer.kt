/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output

object QuickSerializer {
    val kryo = Kryo()

    fun serialize(data: Any?): ByteArray? {
        if (data == null) return null
        val output = Output(YetAnotherFastByteArrayOutputStream())
        output.use {
            kryo.writeObject(output, data)
            return output.toBytes()
        }
    }

    inline fun <reified T> deserialize(data: ByteArray?): T? {
        if (data == null) return null
        return kryo.readObject(Input(data), T::class.java) // can access the reified type directly. thanks, kotlin!
    }
}
