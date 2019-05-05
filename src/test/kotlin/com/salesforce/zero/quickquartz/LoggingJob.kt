/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import org.quartz.Job
import org.quartz.JobExecutionContext
import org.slf4j.LoggerFactory
import java.util.Date
import java.util.concurrent.TimeUnit

/**
 * All this job does is log when it is fired
 */
class LoggingJob : Job {
    private val logger = LoggerFactory.getLogger(LoggingJob::class.java)

    override fun execute(context: JobExecutionContext?) {
        context?.apply {
            val delaySeconds = context.fireTime.minus(context.scheduledFireTime)
            logger.info("${LoggingJob::class.java.simpleName}: delay=$delaySeconds s; job=${context.jobDetail?.key}; data=${context.mergedJobDataMap}")
        }
    }
}

/**
 * get delay in seconds
 */
private fun Date.minus(scheduledFireTime: Date): Long =
    TimeUnit.MILLISECONDS.toSeconds(this.time - scheduledFireTime.time)
