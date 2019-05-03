/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.batchInsert

const val ONE_MiB = 1 * 1024 * 1024

/**
 * schema
 */
object QuickQuartzJobDetails : Table("qrtz_job_details") {
    val schedName = varchar("sched_name", 120).primaryKey(0)
    val jobName = varchar("job_name", 200).primaryKey(1)
    val jobGroup = varchar("job_group", 200).primaryKey(2)
    val description = varchar("description", 250)
    val jobClassName = varchar("job_class_name", 250)
    val isDurable = bool("is_durable")
    val isNonConcurrent = bool("is_nonconcurrent")
    val isUpdateData = bool("is_update_data")
    val requestsRecovery = bool("requests_recovery")
    val jobData = binary("job_data", ONE_MiB).nullable()
}

/**
 * pojo
 */
data class JobDetail(
    val schedName: String,
    val jobName: String,
    val jobGroup: String,
    val description: String,
    val jobClassName: String,
    val isDurable: Boolean,
    val isNonConcurrent: Boolean,
    val isUpdateData: Boolean,
    val requestsRecovery: Boolean,
    val jobData: ByteArray? = byteArrayOf()
)

/**
 * helper that maps JobDetails onto a sql statement
 */
fun Iterable<JobDetail>.batchInsert() {
    QuickQuartzJobDetails.batchInsert(this) { job ->
        this[QuickQuartzJobDetails.schedName] = job.schedName
        this[QuickQuartzJobDetails.jobName] = job.jobName
        this[QuickQuartzJobDetails.jobGroup] = job.jobGroup
        this[QuickQuartzJobDetails.description] = job.description
        this[QuickQuartzJobDetails.jobClassName] = job.jobClassName
        this[QuickQuartzJobDetails.isDurable] = job.isDurable
        this[QuickQuartzJobDetails.isNonConcurrent] = job.isNonConcurrent
        this[QuickQuartzJobDetails.isUpdateData] = job.isUpdateData
        this[QuickQuartzJobDetails.requestsRecovery] = job.requestsRecovery
        this[QuickQuartzJobDetails.jobData] = job.jobData
    }
}

/**
 * helper that maps a result row to a JobDetail
 */
fun ResultRow.toJobDetails(): JobDetail = JobDetail(
    schedName = this[QuickQuartzJobDetails.schedName],
    jobName = this[QuickQuartzJobDetails.jobName],
    jobGroup = this[QuickQuartzJobDetails.jobGroup],
    description = this[QuickQuartzJobDetails.description],
    jobClassName = this[QuickQuartzJobDetails.jobClassName],
    isDurable = this[QuickQuartzJobDetails.isDurable],
    isNonConcurrent = this[QuickQuartzJobDetails.isNonConcurrent],
    isUpdateData = this[QuickQuartzJobDetails.isUpdateData],
    requestsRecovery = this[QuickQuartzJobDetails.requestsRecovery],
    jobData = this[QuickQuartzJobDetails.jobData]
)

