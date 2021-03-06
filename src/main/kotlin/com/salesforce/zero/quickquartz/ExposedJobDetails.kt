/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import com.salesforce.zero.quickquartz.QuickQuartzJobDetails.description
import com.salesforce.zero.quickquartz.QuickQuartzJobDetails.isDurable
import com.salesforce.zero.quickquartz.QuickQuartzJobDetails.isNonConcurrent
import com.salesforce.zero.quickquartz.QuickQuartzJobDetails.isUpdateData
import com.salesforce.zero.quickquartz.QuickQuartzJobDetails.jobClassName
import com.salesforce.zero.quickquartz.QuickQuartzJobDetails.jobData
import com.salesforce.zero.quickquartz.QuickQuartzJobDetails.jobGroup
import com.salesforce.zero.quickquartz.QuickQuartzJobDetails.jobName
import com.salesforce.zero.quickquartz.QuickQuartzJobDetails.requestsRecovery
import com.salesforce.zero.quickquartz.QuickQuartzJobDetails.schedName
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.statements.BatchInsertStatement
import org.quartz.JobDetail

/**
 * schema
 */
object QuickQuartzJobDetails : Table("qrtz_job_details") {
    val schedName = text("sched_name").primaryKey(0)
    val jobName = text("job_name").primaryKey(1)
    val jobGroup = text("job_group").primaryKey(2)
    val description = text("description")
    val jobClassName = text("job_class_name")
    val isDurable = bool("is_durable")
    val isNonConcurrent = bool("is_nonconcurrent")
    val isUpdateData = bool("is_update_data")
    val requestsRecovery = bool("requests_recovery")
    val jobData = jsonb<Map<String, String>>("job_data").nullable()
}

/**
 * poké (plain ol' kotlin entity)
 */
data class JobDetailEntity(
    val schedName: String = "QQ", // TODO just get rid of this from the schema
    val jobName: String,
    val jobGroup: String,
    val description: String = "",
    val jobClassName: String = "",
    val isDurable: Boolean = false,
    val isNonConcurrent: Boolean = false,
    val isUpdateData: Boolean = false,
    val requestsRecovery: Boolean = false,
    val jobData: Map<String, String>? = null
)

/**
 * Helper that maps JobDetails onto a sql statement
 * This is a lambda whose receiver (this) is a BatchInsertStatement.
 * It has one param (a JobDetailEntity) <p/>
 * Usage:
 * ```
 * transaction {
 *     QuickQuartzJobDetails.batchInsert(data = jobs, body = batchInsertJobs)
 * }
 * ```
 */
val batchInsertJobs: BatchInsertStatement.(JobDetailEntity) -> Unit = { job ->
    this[schedName] = job.schedName
    this[jobName] = job.jobName
    this[jobGroup] = job.jobGroup
    this[description] = job.description
    this[jobClassName] = job.jobClassName
    this[isDurable] = job.isDurable
    this[isNonConcurrent] = job.isNonConcurrent
    this[isUpdateData] = job.isUpdateData
    this[requestsRecovery] = job.requestsRecovery
    this[jobData] = job.jobData
}

/**
 * helper that maps a result row to a JobDetailEntity
 * Usage:
 * ```
 * val jobs = QuickQuartzJobDetails.selectAll().map { it.toJobDetails() }
 * ```
 */
fun ResultRow.toJob(): JobDetailEntity = JobDetailEntity(
    schedName = this[schedName],
    jobName = this[jobName],
    jobGroup = this[jobGroup],
    description = this[description],
    jobClassName = this[jobClassName],
    isDurable = this[isDurable],
    isNonConcurrent = this[isNonConcurrent],
    isUpdateData = this[isUpdateData],
    requestsRecovery = this[requestsRecovery],
    jobData = this[jobData]
)

/**
 * maps quartz jobs to quick quartz jobs
 */
val toQuickQuartzJob: JobDetail.() -> JobDetailEntity = {
    val jobData = jobDataMap.takeUnless { it.isEmpty() }?.wrappedMap

    JobDetailEntity(
        jobName = key.name,
        jobGroup = key.group,
        description = description ?: "",
        jobClassName = jobClass.canonicalName,
        isDurable = isDurable,
        isNonConcurrent = isConcurrentExectionDisallowed,
        isUpdateData = isPersistJobDataAfterExecution,
        requestsRecovery = requestsRecovery(),
        jobData = if (jobData == null) null else jobData as Map<String, String> // quartz api claims this is a Map<String, Object>, but in practice this ends up as a Map<String, String>.
    )
}