/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.MoreExecutors
import org.jetbrains.exposed.sql.StdOutSqlLogger
import org.jetbrains.exposed.sql.addLogger
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

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

    @Test
    fun `non blocking row locks with expose`() {
        val prefix = UUID.randomUUID().toString()

        // insert some rows to fight over
        transaction {
            QuickQuartzJobDetails.batchInsert(genQuickQuartzJobsWithTriggers(prefix, 1).keys, body = batchInsertJobs)
        }

        // set up some threads to try and read them back
        val service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2))
        val futures = mutableListOf<ListenableFuture<List<JobDetailEntity>>>()
        val latch = CountDownLatch(2) // one per thread

        // only one thread should be able to win the row level lock
        // the other thread should return immediately
        for (i in 1..2) {
            futures.add(
                service.submit<List<JobDetailEntity>> {
                    transaction {
                        addLogger(StdOutSqlLogger)

                        val jobs = QuickQuartzJobDetails
                            .selectForUpdateSkipLocked { (QuickQuartzJobDetails.jobName like "$prefix%") }
                            .map { it.toJob() }

                        latch.countDown()
                        latch.await(2, TimeUnit.SECONDS)
                        jobs
                    }
                }
            )
        }
        service.shutdown()

        val successful = Futures.successfulAsList(futures).get()
        assertThat(successful.size).isEqualTo(2)

        val first = successful[0]
        val second = successful[1]

        assertWithMessage("first.size = ${first.size}; second.size = ${second.size}")
            .that((first.isEmpty() && second.size == 1).xor(first.size == 1 && second.isEmpty())).isTrue()
    }
}
