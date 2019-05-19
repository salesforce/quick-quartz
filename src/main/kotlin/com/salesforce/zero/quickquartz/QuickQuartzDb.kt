/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import com.salesforce.zero.quickquartz.QuickQuartzTriggers.nextFireTime
import com.salesforce.zero.quickquartz.QuickQuartzTriggers.triggerState
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.StdOutSqlLogger
import org.jetbrains.exposed.sql.addLogger
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
import java.sql.ResultSet
import javax.sql.DataSource

/**
 * One stop shop for all db behavior
 */
class QuickQuartzDb(private val db: DataSource, private val instanceId: String = "defaultSchedulerInstanceId") {

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

    /**
     * - selectForUpdateSkipLocked a batch of triggers, where nextFireTime <= noLaterThan and state = WAITING
     * - update triggerState to ACQUIRED
     * - insert into fired triggers table, with fired instanceId = this scheduler id
     */
    fun acquireNextTriggers(noLaterThan: Long, batchSize: Int = 200, debug: Boolean = false) = transaction {

        if (debug) addLogger(StdOutSqlLogger)

        // acquire row locks optimistically on a batch of eligible triggers
        // TODO: should this filter out paused jobGroups/triggerGroups?
        val triggers = QuickQuartzTriggers
            .selectForUpdateSkipLocked { ((triggerState eq TriggerState.WAITING.name) and (nextFireTime lessEq noLaterThan)) }
            .limit(batchSize)
            .map { it.toTrigger() }

        // grab their IDs
        val triggerIds = triggers.map { it.triggerName }

        // update state to ACQUIRED
        QuickQuartzTriggers.update(where = { QuickQuartzTriggers.triggerName inList triggerIds }) { updateStmt ->
            updateStmt[triggerState] = TriggerState.ACQUIRED.name
        }

        // create fired trigger objects to insert
        val firedTime = System.currentTimeMillis()
        val firedTriggers = triggers.map {
            FiredTriggerEntity(
                entryId = it.triggerName,
                triggerName = it.triggerName,
                triggerGroup = it.triggerGroup,
                firedTime = firedTime,
                schedTime = it.nextFireTime ?: firedTime,
                priority = it.priority ?: 5,
                state = TriggerState.ACQUIRED.name,
                jobName = it.jobName,
                jobGroup = it.jobGroup,
                instanceName = instanceId
            )
        }

        // batch insert the fired triggers
        QuickQuartzFiredTriggers.batchInsert(firedTriggers, body = batchInsertFiredTriggers).map {
            if (debug) println(it.toFiredTrigger())
        }

        // return the triggers we acquired
        triggers
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
