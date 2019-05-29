/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import org.quartz.Scheduler
import org.quartz.core.QuartzScheduler
import org.quartz.core.QuartzSchedulerResources
import org.quartz.impl.StdSchedulerFactory
import java.util.Properties
import java.util.concurrent.atomic.AtomicReference
import javax.sql.DataSource

/**
 * This factory allows injecting properties into the job store during scheduler initialization. E.g., the data source to use.
 */
class QuickQuartzSchedulerFactory(private val dataSource: DataSource, properties: Properties) : StdSchedulerFactory(properties) {
    private val qq: AtomicReference<QuickQuartz> = AtomicReference()

    /**
     * Primary hook to configure the job store
     */
    override fun instantiate(rsrcs: QuartzSchedulerResources?, qs: QuartzScheduler?): Scheduler {
        rsrcs?.apply {
            rsrcs.name = "QuickQuartzScheduler"
            rsrcs.threadName = "QuickQuartzSchedulerThread"
            with((jobStore as QuickQuartz)) {
                initializeQuickQuartzDb(dataSource)
                qq.set(this)
            }
        }
        return super.instantiate(rsrcs, qs)
    }

    /**
     * get a handle to the QuickQuartz job store
     */
    fun quickQuartz(): QuickQuartz = qq.get()
}
