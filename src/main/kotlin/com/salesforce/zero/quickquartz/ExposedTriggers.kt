/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import com.salesforce.zero.quickquartz.QuickQuartzTriggers.calendarName
import com.salesforce.zero.quickquartz.QuickQuartzTriggers.description
import com.salesforce.zero.quickquartz.QuickQuartzTriggers.endTime
import com.salesforce.zero.quickquartz.QuickQuartzTriggers.jobData
import com.salesforce.zero.quickquartz.QuickQuartzTriggers.jobGroup
import com.salesforce.zero.quickquartz.QuickQuartzTriggers.jobName
import com.salesforce.zero.quickquartz.QuickQuartzTriggers.misfireInstr
import com.salesforce.zero.quickquartz.QuickQuartzTriggers.nextFireTime
import com.salesforce.zero.quickquartz.QuickQuartzTriggers.prevFireTime
import com.salesforce.zero.quickquartz.QuickQuartzTriggers.priority
import com.salesforce.zero.quickquartz.QuickQuartzTriggers.schedName
import com.salesforce.zero.quickquartz.QuickQuartzTriggers.startTime
import com.salesforce.zero.quickquartz.QuickQuartzTriggers.triggerGroup
import com.salesforce.zero.quickquartz.QuickQuartzTriggers.triggerName
import com.salesforce.zero.quickquartz.QuickQuartzTriggers.triggerState
import com.salesforce.zero.quickquartz.QuickQuartzTriggers.triggerType
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.statements.BatchInsertStatement
import org.quartz.Trigger

/**
 * the legal states a trigger row can be in
 */
enum class TriggerState {
    WAITING // that's all for now, folks!
}

/**
 * trigger types
 */
enum class TriggerTypes {
    SIMPLE, CRON
}

/**
 * schema for the main Trigger entity
 */
object QuickQuartzTriggers : Table("qrtz_triggers") {
    val schedName = (varchar("sched_name", 120) references QuickQuartzJobDetails.schedName).primaryKey(0)
    val triggerName = varchar("trigger_name", 200).primaryKey(1)
    val triggerGroup = varchar("trigger_group", 200).primaryKey(2)
    val jobName = varchar("job_name", 200) references QuickQuartzJobDetails.jobName
    val jobGroup = varchar("job_group", 200) references QuickQuartzJobDetails.jobGroup

    val description = varchar("description", 250).nullable()
    val nextFireTime = long("next_fire_time").nullable()
    val prevFireTime = long("prev_fire_time").nullable()
    val priority = integer("priority").nullable()

    // TODO enumeration for trigger state?
    val triggerState = varchar("trigger_state", 16)
    val triggerType = varchar("trigger_type", 8)
    val startTime = long("start_time")
    val endTime = long("end_time")

    val calendarName = varchar("calendar_name", 200).nullable()
    val misfireInstr = integer("misfire_instr").nullable()
    val jobData = jsonb<Map<String, String>>("job_data").nullable()
}

/**
 * poké (plain ol' kotlin entity)
 */
data class TriggerEntity(
    val schedName: String = "QQ", // TODO just remove this from the schema
    val triggerName: String,
    val triggerGroup: String,
    val jobName: String,
    val jobGroup: String,

    val description: String? = null,
    val nextFireTime: Long? = null,
    val prevFireTime: Long? = -1, // this is the quartz jdbc default
    val priority: Int? = null,

    val triggerState: String = TriggerState.WAITING.name,
    val triggerType: String = TriggerTypes.SIMPLE.name,
    val startTime: Long = System.currentTimeMillis(),
    val endTime: Long = System.currentTimeMillis(),

    val calendarName: String? = null,
    val misfireInstr: Int? = null,
    val jobData: Map<String, String>? = null
)

/**
 * This is a lambda whose receiver (this) is a BatchInsertStatement.
 * It has one param (a TriggerEntity) and it returns Unit.
 * <p/>
 * Usage:
 * ```
 * transaction {
 *      QuickQuartzTriggers.batchInsert(data = triggers, body = batchInsertTriggers)
 * }
 * ```
 */
val batchInsertTriggers: BatchInsertStatement.(TriggerEntity) -> Unit = { trigger ->
    this[schedName] = trigger.schedName
    this[triggerName] = trigger.triggerName
    this[triggerGroup] = trigger.triggerGroup
    this[jobName] = trigger.jobName
    this[jobGroup] = trigger.jobGroup
    this[description] = trigger.description
    this[nextFireTime] = trigger.nextFireTime
    this[prevFireTime] = trigger.prevFireTime
    this[priority] = trigger.priority
    this[triggerState] = trigger.triggerState
    this[triggerType] = trigger.triggerType
    this[startTime] = trigger.startTime
    this[endTime] = trigger.endTime
    this[calendarName] = trigger.calendarName
    this[misfireInstr] = trigger.misfireInstr
    this[jobData] = trigger.jobData
}

/**
 * helper that reads from a result row to a TriggerEntity
 *
 * Usage:
 * ```
 * (QuickQuartzJobDetails innerJoin QuickQuartzTriggers).selectAll().map { it.toTriggers() }
 * ```
 */
fun ResultRow.toTrigger(): TriggerEntity = TriggerEntity(
    schedName = this[schedName],
    triggerName = this[triggerName],
    triggerGroup = this[triggerGroup],
    jobName = this[jobName],
    jobGroup = this[jobGroup],
    description = this[description],
    nextFireTime = this[nextFireTime],
    prevFireTime = this[prevFireTime],
    priority = this[priority],
    triggerState = this[triggerState],
    triggerType = this[triggerType],
    startTime = this[startTime],
    endTime = this[endTime],
    calendarName = this[calendarName],
    misfireInstr = this[misfireInstr],
    jobData = this[jobData]
)

/**
 * maps a quartz Trigger to TriggerEntity
 */
val toQuickQuartzTrigger: Trigger.() -> TriggerEntity = {
    val jobData = jobDataMap.takeUnless { it.isEmpty() }?.wrappedMap

    TriggerEntity(
        triggerName = this.key.name,
        triggerGroup = this.key.group,
        jobName = this.jobKey.name,
        jobGroup = this.jobKey.group,
        description = this.description,
        nextFireTime = this.nextFireTime?.time,
        prevFireTime = this.previousFireTime?.time,
        priority = this.priority,
        // note that triggerState and triggerType are not supplied by the quartz objects,
        // and so we leave them to their respective default values.
        startTime = this.startTime.time,
        endTime = this.endTime.time,
        calendarName = this.calendarName,
        misfireInstr = this.misfireInstruction,
        jobData = if (jobData == null) null else jobData as Map<String, String>
    )
}
