/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import com.google.common.truth.Truth.assertThat
import com.salesforce.zero.quickquartz.QuickQuartzFiredTriggers.entryId
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.Test
import org.quartz.Scheduler
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.net.InetAddress
import java.time.LocalDate
import java.util.Date
import java.util.UUID
import javax.sql.DataSource
import kotlin.test.fail

@SpringBootTest
class EndToEndTests {

    @Autowired lateinit var scheduler: Scheduler
    @Autowired lateinit var quickQuartz: QuickQuartz
    @Autowired lateinit var db: DataSource

    @Test
    fun useScheduler() {
        assertThat(scheduler.schedulerName).isEqualTo("QuickQuartzScheduler")
        assertThat(scheduler.schedulerInstanceId).startsWith(InetAddress.getLocalHost().hostName)
    }

    @Test
    fun `store jobs and triggers`() {
        // given
        val prefix = UUID.randomUUID().toString()

        // when
        scheduler.scheduleJobs(genQuartzJobs(prefix), false)

        // then make sure the rows ended up where we sent 'em
        verifyJobsAndTriggers(prefix)
    }

    @Test
    fun `schedule single job`() {
        val prefix = UUID.randomUUID().toString()
        val jobToTriggers = genQuartzJobs(prefix, payload = mapOf("k1" to "v1"))
        val job = jobToTriggers.keys.elementAt(0)
        val trigger = jobToTriggers[job]?.elementAt(0) ?: fail()

        scheduler.scheduleJob(job, trigger)

        verifyJobsAndTriggers(prefix, expectedCount = 1)
    }

    /**
     * test for `[org.quartz.core.QuartzScheduler.addJob(org.quartz.JobDetail, boolean, boolean)]`
     */
    @Test
    fun `schedule job with no trigger`() {
        val prefix = UUID.randomUUID().toString()
        val jobToTriggers = genQuartzJobs(prefix)
        val job = jobToTriggers.keys.elementAt(0)

        scheduler.addJob(job, false)

        transaction {
            val list = QuickQuartzJobDetails.select { QuickQuartzJobDetails.jobName like "$prefix%" }.map { it.toJob() }
            assertThat(list.size).isEqualTo(1)
        }
    }

    /**
     * this does a join so both job and detail rows must be present for this to pass
     */
    private fun verifyJobsAndTriggers(prefix: String, expectedCount: Int = TEST_DEFAULT_NUM_ENTITIES) {
        transaction {
            val jobsAndTriggers = (QuickQuartzJobDetails innerJoin QuickQuartzTriggers)
                .select { (QuickQuartzJobDetails.schedName eq(TEST_SCHEDULER_NAME)) and (QuickQuartzJobDetails.jobName like "$prefix%") }
                .orderBy(QuickQuartzJobDetails.jobName)
                .map { it.toJob() to it.toTrigger() }

            assertThat(jobsAndTriggers.size).isEqualTo(expectedCount)

            // all triggers have a legit nextFireTime
            jobsAndTriggers.forEach {
                val trigger = it.second
                assertThat(trigger.nextFireTime).isGreaterThan(LocalDate.now().toEpochDay())
            }
        }
    }

    @Test
    fun `acquire next triggers from quartzdb`() {
        // given
        val prefix = UUID.randomUUID().toString()
        val quartzJobs = genQuartzJobs(prefix)
        val quartzDb = QuickQuartzDb(db, prefix)
        scheduler.scheduleJobs(quartzJobs, false)
        verifyJobsAndTriggers(prefix)

        // when
        val nextTriggers = quartzDb.acquireNextTriggers(Date().time, debug = true, tenantFilter = QuickQuartzTriggers.triggerGroup eq prefix)

        // then
        assertThat(nextTriggers).isNotEmpty()
        assertThat(nextTriggers.filter { it.triggerName.startsWith(prefix) }).hasSize(TEST_DEFAULT_NUM_ENTITIES)

        // should have inserted into the fired triggers table
        transaction {
            val fired = QuickQuartzFiredTriggers.select { entryId like "$prefix%" }.map { it.toFiredTrigger() }
            assertThat(fired.size).isEqualTo(TEST_DEFAULT_NUM_ENTITIES)
            fired.forEach {
                assertThat(it.instanceName).isEqualTo(prefix)
                assertThat(it.state).isEqualTo(TriggerState.ACQUIRED.name)
            }
        }

        // oh and also check that converting it to a quartz trigger preserves the nextFireTime
        assertThat(nextTriggers.first().toOperableTrigger().nextFireTime).isEqualTo(Date(nextTriggers.first().nextFireTime!!))
    }

    @Test
    fun `acquire next triggers via qq`() {
        // given
        val batchSize = 2
        val prefix = UUID.randomUUID().toString()
        scheduler.scheduleJobs(genQuartzJobs(prefix), false)

        // when
        val operableTriggers = quickQuartz.acquireNextTriggers(System.currentTimeMillis(), batchSize, -1)

        assertThat(operableTriggers).hasSize(batchSize)
    }
}