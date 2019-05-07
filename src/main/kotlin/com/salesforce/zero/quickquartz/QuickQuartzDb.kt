/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.transactions.transaction
import java.sql.ResultSet
import javax.sql.DataSource

/**
 * One stop shop for all db behavior
 */
class QuickQuartzDb(private val db: DataSource) {

    init {
        db.connection.use {
            if (it.autoCommit) throw Error("please turn off autocommit")
        }

        // initialize Expose
        Database.connect(this.db)
    }

    /**
     * read the version from the database
     */
    fun readPgVersion(): String {
        db.connection.use { conn ->
            conn.createStatement().use { stmt ->
                // verify that we are using postgres 10.6
                stmt.executeQuery("select version()").use { rs ->
                    val list = rs.toResultsList { getString(1) }
                    return list[0]
                }
            }
        }
    }

    /**
     * persist jobs and associated triggers in bulk
     */
    fun batchInsertJobsAndDetails(qqJobs: List<JobDetailEntity>, qqTriggers: Iterable<TriggerEntity>) {
        transaction {
            // insert parent rows
            QuickQuartzJobDetails.batchInsert(data = qqJobs, body = batchInsertJobs)

            // insert child rows
            QuickQuartzTriggers.batchInsert(data = qqTriggers, body = batchInsertTriggers)
        }
    }
}

/**
 * a handy extension function that applies the given lambda fn to each row in the resultSet and transforms it into a list
 * think: Iterables.transform() except that ResultSet is not an iterable :)
 */
inline fun <T> ResultSet.toResultsList(fn: ResultSet.() -> T): List<T> =
    mutableListOf<T>().apply {
        while (next()) {
            add(fn(this@toResultsList))
        }
    }
