/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import com.google.common.truth.Truth.assertThat
import org.jetbrains.exposed.sql.StdOutSqlLogger
import org.jetbrains.exposed.sql.addLogger
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import java.util.UUID

@SpringBootTest
class ExposedTests {

    @Test
    fun `bulk insert job details`() {
        val prefix = UUID.randomUUID().toString()

        // cook up some objects to insert
        val inputJobs = mutableListOf<JobDetailEntity>().apply {
            for (i in 1..5)
                add(JobDetailEntity(schedName = TEST_SCHEDULER_NAME, jobName = "$prefix-$i", jobGroup = "$i"))
        }

        // insert them in one batch and commit
        transaction {
            addLogger(StdOutSqlLogger)
            QuickQuartzJobDetails.batchInsert(data = inputJobs, body = batchInsertJobs)
        }

        // read back from another transaction
        transaction {
            addLogger(StdOutSqlLogger)
            val inserted = QuickQuartzJobDetails.select {
                QuickQuartzJobDetails.jobName like "$prefix-%"
            }.map { it.toJob() }

            assertThat(inserted.size).isEqualTo(5)
            assertThat(inserted.map { it.jobName }).isEqualTo(inputJobs.map { it.jobName })
        }
    }

    /**
     * batch insert parent-child rows
     */
    @Test
    fun `bulk insert jobs and triggers`() {
        val prefix = UUID.randomUUID().toString()
        val jobsToTriggers = genQuickQuartzJobsWithTriggers(prefix)

        transaction {
            addLogger(StdOutSqlLogger)

            // insert parent rows
            QuickQuartzJobDetails.batchInsert(data = jobsToTriggers.keys, body = batchInsertJobs)

            // insert child rows
            QuickQuartzTriggers.batchInsert(data = jobsToTriggers.values, body = batchInsertTriggers)

            // inner join to read them all back
            val listOfTriggers = (QuickQuartzJobDetails innerJoin QuickQuartzTriggers)
                .select { (QuickQuartzJobDetails.schedName eq(TEST_SCHEDULER_NAME)) and (QuickQuartzJobDetails.jobName like "$prefix%") }
                .orderBy(QuickQuartzJobDetails.jobName)
                .map { it.toTrigger() }

            assertThat(listOfTriggers.size).isEqualTo(TEST_DEFAULT_NUM_ENTITIES)
            assertThat(listOfTriggers).isEqualTo(jobsToTriggers.values.sortedBy { it.triggerName }.toList())
        }
    }
}
