/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.jetbrains.exposed.sql.Column
import org.jetbrains.exposed.sql.ColumnType
import org.jetbrains.exposed.sql.FieldSet
import org.jetbrains.exposed.sql.Op
import org.jetbrains.exposed.sql.Query
import org.jetbrains.exposed.sql.SqlExpressionBuilder
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.Transaction
import org.postgresql.util.PGobject
import java.sql.PreparedStatement
import java.sql.Types

/**
 * `SELECT ... FOR UPDATE SKIP LOCKED`
 *
 * this is handy extension function that can be applied to any table (which is a [FieldSet]
 *
 * Usage:
 * ```
 * QuickQuartzJobDetails
 * .selectForUpdateSkipLocked { (QuickQuartzJobDetails.jobName like "$prefix%") }
 * .map { it.toJob() }
 * ```
 */
inline fun FieldSet.selectForUpdateSkipLocked(where: SqlExpressionBuilder.() -> Op<Boolean>): Query =
    selectForUpdateSkipLocked(SqlExpressionBuilder.where())

/**
 * this creates an instance of our custom [SkipLockedQuery]
 */
fun FieldSet.selectForUpdateSkipLocked(where: Op<Boolean>): Query = SkipLockedQuery(this, where)

/**
 * Query implementation that tacks on a `for update skip locked` to the original query
 */
open class SkipLockedQuery(set: FieldSet, where: Op<Boolean>?) : Query(set, where) {
    override fun prepareSQL(transaction: Transaction): String {
        val sql = super.prepareSQL(transaction)
        return "$sql for update skip locked"
    }
}

/**
 * JSONB support
 */
fun <T : Map<String, String>> Table.jsonb(name: String): Column<T> =
    registerColumn(name, Json())

private class Json : ColumnType() {
    companion object {
        private val gson = Gson()
        private val typeOfHashMap = object : TypeToken<Map<String, String>>() {}.type
        const val TYPE = "jsonb"
    }

    override fun sqlType(): String = TYPE

    override fun setParameter(stmt: PreparedStatement, index: Int, value: Any?) {
        if (value == null) {
            stmt.setNull(index, Types.OTHER)
        } else {
            val obj = PGobject()
            obj.type = TYPE
            obj.value = value as String
            stmt.setObject(index, obj)
        }
    }

    override fun valueFromDB(value: Any): Any {
        if (value !is PGobject) throw Exception("value was $value of type ${value.javaClass}")

        return try {
            gson.fromJson(value.value, typeOfHashMap)
        } catch (e: Exception) {
            throw Exception("Can't parse JSON: $value", e)
        }
    }

    override fun notNullValueToDB(value: Any): Any = gson.toJson(value)
    override fun nonNullValueToString(value: Any): String = "'${gson.toJson(value)}'"
}
