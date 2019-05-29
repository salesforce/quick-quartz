/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import com.salesforce.zero.quickquartz.QuickQuartzFiredTriggers.entryId
import com.salesforce.zero.quickquartz.QuickQuartzFiredTriggers.firedTime
import com.salesforce.zero.quickquartz.QuickQuartzFiredTriggers.instanceName
import com.salesforce.zero.quickquartz.QuickQuartzFiredTriggers.isNonConcurrent
import com.salesforce.zero.quickquartz.QuickQuartzFiredTriggers.jobGroup
import com.salesforce.zero.quickquartz.QuickQuartzFiredTriggers.jobName
import com.salesforce.zero.quickquartz.QuickQuartzFiredTriggers.priority
import com.salesforce.zero.quickquartz.QuickQuartzFiredTriggers.requestsRecovery
import com.salesforce.zero.quickquartz.QuickQuartzFiredTriggers.schedName
import com.salesforce.zero.quickquartz.QuickQuartzFiredTriggers.schedTime
import com.salesforce.zero.quickquartz.QuickQuartzFiredTriggers.state
import com.salesforce.zero.quickquartz.QuickQuartzFiredTriggers.triggerGroup
import com.salesforce.zero.quickquartz.QuickQuartzFiredTriggers.triggerName
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.statements.BatchInsertStatement

/**
 * schema
 */
object QuickQuartzFiredTriggers : Table("qrtz_fired_triggers") {
    val schedName = text("sched_name").primaryKey(0)
    val entryId = text("entry_id").primaryKey(1)
    val triggerName = text("trigger_name")
    val triggerGroup = text("trigger_group")
    val instanceName = text("instance_name")
    val firedTime = long("fired_time")
    val schedTime = long("sched_time")
    val priority = integer("priority")
    val state = text("state")
    val jobName = text("job_name").nullable()
    val jobGroup = text("job_group").nullable()
    val isNonConcurrent = bool("is_nonconcurrent").nullable()
    val requestsRecovery = bool("requests_recovery").nullable()
}

/**
 * poké (plain ol' kotlin entity)
 */
data class FiredTriggerEntity(
    val schedName: String = "QQ", // TODO just remove this from the schema
    val entryId: String,
    val triggerName: String,
    val triggerGroup: String,
    val instanceName: String,
    val firedTime: Long,
    val schedTime: Long,
    val priority: Int,
    val state: String,
    val jobName: String?,
    val jobGroup: String?,
    val isNonConcurrent: Boolean? = false,
    val requestsRecovery: Boolean? = false
)

/**
 * populate a batch insert statement's fields with a FiredTriggerEntity
 */
val batchInsertFiredTriggers: BatchInsertStatement.(FiredTriggerEntity) -> Unit = {
    this[schedName] = it.schedName
    this[entryId] = it.entryId
    this[triggerName] = it.triggerName
    this[triggerGroup] = it.triggerGroup
    this[instanceName] = it.instanceName
    this[firedTime] = it.firedTime
    this[schedTime] = it.schedTime
    this[priority] = it.priority
    this[state] = it.state
    this[jobName] = it.jobName
    this[jobGroup] = it.jobGroup
    this[isNonConcurrent] = it.isNonConcurrent
    this[requestsRecovery] = it.requestsRecovery
}

/**
 * read a FiredTriggerEntity from a result row
 */
fun ResultRow.toFiredTrigger(): FiredTriggerEntity = FiredTriggerEntity(
    schedName = this[schedName],
    entryId = this[entryId],
    triggerName = this[triggerName],
    triggerGroup = this[triggerGroup],
    instanceName = this[instanceName],
    firedTime = this[firedTime],
    schedTime = this[schedTime],
    priority = this[priority],
    state = this[state],
    jobName = this[jobName],
    jobGroup = this[jobGroup],
    isNonConcurrent = this[isNonConcurrent],
    requestsRecovery = this[requestsRecovery]
)
