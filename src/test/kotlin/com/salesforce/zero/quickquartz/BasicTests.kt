/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import com.google.common.truth.Truth.assertThat
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.quartz.JobBuilder
import org.quartz.JobDataMap
import org.quartz.JobDetail
import org.quartz.SimpleScheduleBuilder
import org.quartz.Trigger
import org.quartz.TriggerBuilder
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.Date

class BasicTests {
    private val prefix = BasicTests::class.java.simpleName

    /**
     * the scheduler instance is going to hand us a list of quartz jobDetails,
     * which need to be transformed into QuickQuartz entities, including
     * serializing the data payload, if any
     */
    @ParameterizedTest
    @MethodSource("payloads")
    fun `map quartz job to quick quartz job`(payload: Map<String, String>?) {
        // given
        val quartzJobs: Iterable<JobDetail> = genQuartzJobs(prefix, payload = payload).keys
        val expected = genQuickQuartzJobsWithTriggers(prefix, payload = payload).keys.toList()

        // when
        val quickQuartzJobs = quartzJobs.map { it.toQuickQuartzJob() }

        // then
        assertThat(quickQuartzJobs).isEqualTo(expected)
    }

    @ParameterizedTest
    @MethodSource("payloads")
    fun `test trigger payload equality`(payload: Map<String, String>?) {
        val triggers = genQuickQuartzJobsWithTriggers(prefix, payload = payload).values.toList()
        triggers.forEach {
            assertThat(it.jobData).isEqualTo(payload)
        }
    }

    @Test
    fun `simple trigger equality`() {
        val t1 = TriggerEntity(schedName = TEST_SCHEDULER_NAME, triggerName = "t1", triggerGroup = "test", jobGroup = "test", jobName = "j1", startTime = 0, endTime = 0)
        val t2 = TriggerEntity(schedName = TEST_SCHEDULER_NAME, triggerName = "t1", triggerGroup = "test", jobGroup = "test", jobName = "j1", startTime = 0, endTime = 0)
        val t3 = TriggerEntity(schedName = TEST_SCHEDULER_NAME, triggerName = "t1", triggerGroup = "test", jobGroup = "test", jobName = "j1", startTime = 1, endTime = 1, jobData = mapOf())
        assertThat(t1).isEqualTo(t2)
        assertThat(t1).isNotEqualTo(t3)
    }

    @Test
    fun `serialize maps with gson`() {
        val gson = Gson()
        val map = mapOf("k1" to "v1")
        val json = gson.toJson(map)
        val typeOfHashMap = object : TypeToken<Map<String, String>>() {}.type

        val deserialized: Map<String, String> = gson.fromJson(json, typeOfHashMap)
        assertThat(deserialized).isEqualTo(map)
    }

    companion object {
        @JvmStatic
        fun payloads() = arrayOf(
            Arguments.of(null),
            Arguments.of(mapOf("k1" to "v1"))
        )
    }
}

/**
 * Generates a map of regular quartz JobDetails to Triggers.
 * @param prefix each job key starts with the given prefix
 * @param numEntities the number of entities to generate
 * @param clazz the job type, LoggingJob by default
 * @param payload pluggable, but null by default.
 * @see LoggingJob
 */
fun genQuartzJobs(
    prefix: String,
    numEntities: Int = TEST_DEFAULT_NUM_ENTITIES,
    clazz: Class<LoggingJob> = LoggingJob::class.java,
    payload: Map<String, String>? = null
) = mutableMapOf<JobDetail, MutableSet<out Trigger>>().apply {
    for (i in 1..numEntities) {
        val jobBuilder = JobBuilder.newJob()
            .ofType(clazz)
            .storeDurably()
            .withIdentity("$prefix-$i", "testJobGroup")
        if (payload != null) jobBuilder.setJobData(JobDataMap(payload))
        val job = jobBuilder.build()

        val triggerBuilder = TriggerBuilder.newTrigger()
            .forJob(job)
            .withIdentity(job.key.name, "testGroup")
            .startNow()
            .endAt(Date.from(LocalDateTime.now().plusMinutes(1).toInstant(ZoneOffset.UTC)))
            .withSchedule(SimpleScheduleBuilder.simpleSchedule().withMisfireHandlingInstructionFireNow())
        // TODO leave out the payload on the trigger for now - for recurrences it will go on the trigger rather than the job
        val trigger: Trigger = triggerBuilder.build()
        put(job, mutableSetOf(trigger))
    }
}

/**
 * Generates a map of QuickQuartz job detail to trigger entities
 * @param prefix each (job|trigger) key starts with the given prefix
 * @param numEntities the number of entities to generate
 * @param jobClassName the job class; by default this is LoggingJob
 * @param payload pluggable payload - set on both parent and child rows (default:null)
 *
 * @see LoggingJob
 */
fun genQuickQuartzJobsWithTriggers(
    prefix: String,
    numEntities: Int = TEST_DEFAULT_NUM_ENTITIES,
    jobClassName: String = "com.salesforce.zero.quickquartz.LoggingJob",
    schedulerName: String = TEST_SCHEDULER_NAME,
    payload: Map<String, String>? = null
): MutableMap<JobDetailEntity, TriggerEntity> = mutableMapOf<JobDetailEntity, TriggerEntity>().apply {
    for (i in 1..numEntities) {
        val detail = JobDetailEntity(
            schedName = schedulerName,
            jobName = "$prefix-$i",
            jobGroup = "testJobGroup",
            jobClassName = jobClassName,
            jobData = if (payload == null) null else payload,
            isDurable = true
        )
        val trigger = TriggerEntity(
            schedName = schedulerName,
            triggerName = "$prefix-$i",
            triggerGroup = "testTriggerGroup",
            jobName = "$prefix-$i",
            jobGroup = "testJobGroup",
            jobData = if (payload == null) null else payload
        )
        put(detail, trigger)
    }
}

const val TEST_SCHEDULER_NAME = "QQ"
const val TEST_DEFAULT_NUM_ENTITIES = 10
