/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import org.jetbrains.exposed.sql.FieldSet
import org.jetbrains.exposed.sql.Op
import org.jetbrains.exposed.sql.Query
import org.jetbrains.exposed.sql.SqlExpressionBuilder
import org.jetbrains.exposed.sql.Transaction

/**
 * `SELECT ... FOR UPDATE SKIP LOCKED`
 *
 * this is handy extension function that can be applied to any table (which is a [FieldSet]
 *
 * Usage:
 * ```
 * QuickQuartzJobDetails
 * .selectForUpdateSkipLocked { (QuickQuartzJobDetails.jobName like "$prefix%") }
 * .map { it.toJob() }
 * ```
 */
inline fun FieldSet.selectForUpdateSkipLocked(where: SqlExpressionBuilder.() -> Op<Boolean>): Query =
    selectForUpdateSkipLocked(SqlExpressionBuilder.where())

/**
 * this creates an instance of our custom [SkipLockedQuery]
 */
fun FieldSet.selectForUpdateSkipLocked(where: Op<Boolean>): Query = SkipLockedQuery(this, where)

/**
 * Query implementation that tacks on a `for update skip locked` to the original query
 */
open class SkipLockedQuery(set: FieldSet, where: Op<Boolean>?) : Query(set, where) {
    override fun prepareSQL(transaction: Transaction): String {
        val sql = super.prepareSQL(transaction)
        return "$sql for update skip locked"
    }
}
