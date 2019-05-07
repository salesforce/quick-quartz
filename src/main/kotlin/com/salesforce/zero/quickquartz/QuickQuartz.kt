/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.transactions.transaction
import org.quartz.Calendar
import org.quartz.JobDetail
import org.quartz.JobKey
import org.quartz.Trigger
import org.quartz.TriggerKey
import org.quartz.impl.jdbcjobstore.TablePrefixAware
import org.quartz.impl.matchers.GroupMatcher
import org.quartz.spi.ClassLoadHelper
import org.quartz.spi.JobStore
import org.quartz.spi.OperableTrigger
import org.quartz.spi.SchedulerSignaler
import org.quartz.spi.TriggerFiredResult
import org.slf4j.LoggerFactory
import javax.sql.DataSource

class QuickQuartz : JobStore, TablePrefixAware {
    companion object {
        private val logger = LoggerFactory.getLogger(QuickQuartz::class.java)
    }

    private lateinit var db: QuickQuartzDb
    private lateinit var instanceId: String
    private lateinit var instanceName: String
    private lateinit var tablePrefix: String
    private var poolSize: Int = 4

    /**
     * Needs to be called prior to scheduler initialization
     */
    fun initializeQuickQuartzDb(dataSource: DataSource) {
        this.db = QuickQuartzDb(dataSource)
    }

    /**
     * Inform the `JobStore` that the scheduler no longer plans to
     * fire the given `Trigger`, that it had previously acquired
     * (reserved).
     */
    override fun releaseAcquiredTrigger(trigger: OperableTrigger?) {
        TODO("not implemented")
    }

    /**
     * Remove (delete) the `[org.quartz.Calendar]` with the
     * given name.
     *
     *
     *
     * If removal of the `Calendar` would result in
     * `Trigger`s pointing to non-existent calendars, then a
     * `JobPersistenceException` will be thrown.
     * *
     * @param calName The name of the `Calendar` to be removed.
     * @return `true` if a `Calendar` with the given name
     * was found and removed from the store.
     */
    override fun removeCalendar(calName: String?): Boolean {
        TODO("not implemented")
    }

    /**
     * How long (in milliseconds) the `JobStore` implementation
     * estimates that it will take to release a trigger and acquire a new one.
     */
    override fun getEstimatedTimeToReleaseAndAcquireTrigger(): Long {
        TODO("not implemented")
    }

    /**
     * Inform the `JobStore` that the scheduler is now firing the
     * given `Trigger` (executing its associated `Job`),
     * that it had previously acquired (reserved).
     *
     * @return may return null if all the triggers or their calendars no longer exist, or
     * if the trigger was not successfully put into the 'executing'
     * state.  Preference is to return an empty list if none of the triggers
     * could be fired.
     */
    override fun triggersFired(triggers: MutableList<OperableTrigger>?): MutableList<TriggerFiredResult> {
        TODO("not implemented")
    }

    /**
     * Determine whether a [Job] with the given identifier already
     * exists within the scheduler.
     *
     * @param jobKey the identifier to check for
     * @return true if a Job exists with the given identifier
     * @throws SchedulerException
     */
    override fun checkExists(jobKey: JobKey?): Boolean {
        TODO("not implemented")
    }

    /**
     * Determine whether a [Trigger] with the given identifier already
     * exists within the scheduler.
     *
     * @param triggerKey the identifier to check for
     * @return true if a Trigger exists with the given identifier
     * @throws SchedulerException
     */
    override fun checkExists(triggerKey: TriggerKey?): Boolean {
        TODO("not implemented")
    }

    /**
     * Store the given `[org.quartz.JobDetail]` and `[org.quartz.Trigger]`.
     *
     * @param newJob
     * The `JobDetail` to be stored.
     * @param newTrigger
     * The `Trigger` to be stored.
     * @throws ObjectAlreadyExistsException
     * if a `Job` with the same name/group already
     * exists.
     */
    override fun storeJobAndTrigger(newJob: JobDetail?, newTrigger: OperableTrigger?) {
        val job = newJob ?: throw IllegalArgumentException("job was null")
        val trigger = newTrigger ?: throw IllegalArgumentException("trigger was null")
        storeJobsAndTriggers(mutableMapOf(job to mutableSetOf(trigger)), replace = false)
    }

    /**
     * Resume (un-pause) all triggers - equivalent of calling `resumeTriggerGroup(group)`
     * on every group.
     *
     *
     *
     * If any `Trigger` missed one or more fire-times, then the
     * `Trigger`'s misfire instruction will be applied.
     *
     *
     * @see .pauseAll
     */
    override fun resumeAll() {
        TODO("not implemented")
    }

    override fun removeJobs(jobKeys: MutableList<JobKey>?): Boolean {
        TODO("not implemented")
    }

    /**
     * Inform the `JobStore` that the scheduler has completed the
     * firing of the given `Trigger` (and the execution of its
     * associated `Job` completed, threw an exception, or was vetoed),
     * and that the `[org.quartz.JobDataMap]`
     * in the given `JobDetail` should be updated if the `Job`
     * is stateful.
     */
    override fun triggeredJobComplete(
        trigger: OperableTrigger?,
        jobDetail: JobDetail?,
        triggerInstCode: Trigger.CompletedExecutionInstruction?
    ) {
        TODO("not implemented")
    }

    /**
     * Pause all of the `[org.quartz.Job]s` in the given
     * group - by pausing all of their `Trigger`s.
     *
     *
     *
     * The JobStore should "remember" that the group is paused, and impose the
     * pause on any new jobs that are added to the group while the group is
     * paused.
     *
     *
     * @see .resumeJobs
     */
    override fun pauseJobs(groupMatcher: GroupMatcher<JobKey>?): MutableCollection<String> {
        TODO("not implemented")
    }

    /**
     * Get the names of all of the `[org.quartz.Trigger]` s
     * that have the given group name.
     *
     *
     *
     * If there are no triggers in the given group name, the result should be a
     * zero-length array (not `null`).
     *
     */
    override fun getTriggerKeys(matcher: GroupMatcher<TriggerKey>?): MutableSet<TriggerKey> {
        TODO("not implemented")
    }

    /**
     * Pause the `[org.quartz.Job]` with the given name - by
     * pausing all of its current `Trigger`s.
     *
     * @see .resumeJob
     */
    override fun pauseJob(jobKey: JobKey?) {
        TODO("not implemented")
    }

    /**
     * Pause all of the `[org.quartz.Trigger]s` in the
     * given group.
     *
     *
     *
     *
     * The JobStore should "remember" that the group is paused, and impose the
     * pause on any new triggers that are added to the group while the group is
     * paused.
     *
     *
     * @see .resumeTriggers
     */
    override fun pauseTriggers(matcher: GroupMatcher<TriggerKey>?): MutableCollection<String> {
        TODO("not implemented")
    }

    /**
     * Get the number of `[org.quartz.Job]` s that are
     * stored in the `JobsStore`.
     */
    override fun getNumberOfJobs(): Int {
        TODO("not implemented")
    }

    /**
     * Reset the current state of the identified `[Trigger]`
     * from [TriggerState.ERROR] to [TriggerState.NORMAL] or
     * [TriggerState.PAUSED] as appropriate.
     *
     *
     * Only affects triggers that are in ERROR state - if identified trigger is not
     * in that state then the result is a no-op.
     *
     *
     * The result will be the trigger returning to the normal, waiting to
     * be fired state, unless the trigger's group has been paused, in which
     * case it will go into the PAUSED state.
     */
    override fun resetTriggerFromErrorState(triggerKey: TriggerKey?) {
        TODO("not implemented")
    }

    /**
     * Called by the QuartzScheduler to inform the `JobStore` that
     * the scheduler has resumed after being paused.
     */
    override fun schedulerResumed() {
        TODO("not implemented")
    }

    /**
     * Resume (un-pause) all of the `[org.quartz.Trigger]s`
     * in the given group.
     *
     *
     *
     * If any `Trigger` missed one or more fire-times, then the
     * `Trigger`'s misfire instruction will be applied.
     *
     *
     * @see .pauseTriggers
     */
    override fun resumeTriggers(matcher: GroupMatcher<TriggerKey>?): MutableCollection<String> {
        TODO("not implemented")
    }

    /**
     * Resume (un-pause) all of the `[org.quartz.Job]s` in
     * the given group.
     *
     *
     *
     * If any of the `Job` s had `Trigger` s that
     * missed one or more fire-times, then the `Trigger`'s
     * misfire instruction will be applied.
     *
     *
     * @see .pauseJobs
     */
    override fun resumeJobs(matcher: GroupMatcher<JobKey>?): MutableCollection<String> {
        TODO("not implemented")
    }

    /**
     * Called by the QuartzScheduler to inform the `JobStore` that
     * the scheduler has been paused.
     */
    override fun schedulerPaused() {
        // FIXME
        logger.warn("scheduler $instanceName paused")
        // TODO("not implemented")
    }

    /**
     * Inform the `JobStore` of the Scheduler instance's name,
     * prior to initialize being invoked.
     *
     * @since 1.7
     */
    override fun setInstanceName(schedName: String?) {
        this.instanceName = schedName ?: throw Error("scheduler name is required")
    }

    /**
     * Retrieve the given `[org.quartz.Trigger]`.
     *
     * @param calName
     * The name of the `Calendar` to be retrieved.
     * @return The desired `Calendar`, or null if there is no
     * match.
     */
    override fun retrieveCalendar(calName: String?): Calendar {
        TODO("not implemented")
    }

    /**
     * Store the given `[org.quartz.JobDetail]`.
     *
     * @param newJob
     * The `JobDetail` to be stored.
     * @param replaceExisting
     * If `true`, any `Job` existing in the
     * `JobStore` with the same name & group should be
     * over-written.
     * @throws ObjectAlreadyExistsException
     * if a `Job` with the same name/group already
     * exists, and replaceExisting is set to false.
     */
    override fun storeJob(newJob: JobDetail?, replaceExisting: Boolean) {
        val job = newJob ?: throw IllegalArgumentException("job was null")
        storeJobsAndTriggers(mutableMapOf(job to mutableSetOf()), replace = false)
    }

    override fun removeTriggers(triggerKeys: MutableList<TriggerKey>?): Boolean {
        TODO("not implemented")
    }

    /**
     * Get the number of `[org.quartz.Calendar]` s that are
     * stored in the `JobsStore`.
     */
    override fun getNumberOfCalendars(): Int {
        TODO("not implemented")
    }

    /**
     * Get the keys of all of the `[org.quartz.Job]` s that
     * have the given group name.
     *
     *
     *
     * If there are no jobs in the given group name, the result should be
     * an empty collection (not `null`).
     *
     */
    override fun getJobKeys(matcher: GroupMatcher<JobKey>?): MutableSet<JobKey> {
        TODO("not implemented")
    }

    /**
     * Get a handle to the next trigger to be fired, and mark it as 'reserved'
     * by the calling scheduler.
     *
     * @param noLaterThan If > 0, the JobStore should only return a Trigger
     * that will fire no later than the time represented in this value as
     * milliseconds.
     * @see .releaseAcquiredTrigger
     */
    override fun acquireNextTriggers(noLaterThan: Long, maxCount: Int, timeWindow: Long): MutableList<OperableTrigger> {
        TODO("not implemented")
    }

    /**
     * Get the names of all of the `[org.quartz.Job]`
     * groups.
     *
     *
     *
     * If there are no known group names, the result should be a zero-length
     * array (not `null`).
     *
     */
    override fun getJobGroupNames(): MutableList<String> {
        TODO("not implemented")
    }

    /**
     * Pause the `[org.quartz.Trigger]` with the given key.
     *
     * @see .resumeTrigger
     */
    override fun pauseTrigger(triggerKey: TriggerKey?) {
        TODO("not implemented")
    }

    override fun storeJobsAndTriggers(triggersAndJobs: MutableMap<JobDetail, MutableSet<out Trigger>>?, replace: Boolean) {
        triggersAndJobs?.apply {

            val jobDetails = this.keys
            val qqJobs = jobDetails.map { it.toQuickQuartzJob() }

            val triggers = this.values.flatten()
            val qqTriggers: Iterable<TriggerEntity> = triggers.map { it.toQuickQuartzTrigger() }

            db.batchInsertJobsAndDetails(qqJobs, qqTriggers)
        }

        // TODO("handle the replace param?")
    }

    /**
     * Get the number of `[org.quartz.Trigger]` s that are
     * stored in the `JobsStore`.
     */
    override fun getNumberOfTriggers(): Int {
        TODO("not implemented")
    }

    override fun supportsPersistence(): Boolean = true

    /**
     * Tells the JobStore the pool size used to execute jobs
     * @param poolSize amount of threads allocated for job execution
     * @since 2.0
     */
    override fun setThreadPoolSize(poolSize: Int) {
        this.poolSize = poolSize
    }

    /**
     * Inform the `JobStore` of the Scheduler instance's Id,
     * prior to initialize being invoked.
     *
     * @since 1.7
     */
    override fun setInstanceId(schedInstId: String?) {
        this.instanceId = schedInstId ?: throw Error("instanceId is required")
    }

    /**
     * Get the amount of time (in ms) to wait when accessing this job store
     * repeatedly fails.
     *
     * Called by the executor thread(s) when calls to
     * [.acquireNextTriggers] fail more than once in succession, and the
     * thread thus wants to wait a bit before trying again, to not consume
     * 100% CPU, write huge amounts of errors into logs, etc. in cases like
     * the DB being offline/restarting.
     *
     * The delay returned by implementations should be between 20 and
     * 600000 milliseconds.
     *
     * @param failureCount the number of successive failures seen so far
     * @return the time (in milliseconds) to wait before trying again
     */
    override fun getAcquireRetryDelay(failureCount: Int): Long {
        TODO("not implemented")
    }

    /**
     * Remove (delete) the `[org.quartz.Trigger]` with the
     * given key.
     *
     *
     *
     * If removal of the `Trigger` results in an empty group, the
     * group should be removed from the `JobStore`'s list of
     * known group names.
     *
     *
     *
     *
     * If removal of the `Trigger` results in an 'orphaned' `Job`
     * that is not 'durable', then the `Job` should be deleted
     * also.
     *
     *
     * @return `true` if a `Trigger` with the given
     * name & group was found and removed from the store.
     */
    override fun removeTrigger(triggerKey: TriggerKey?): Boolean {
        TODO("not implemented")
    }

    /**
     * Store the given `[org.quartz.Trigger]`.
     *
     * @param newTrigger
     * The `Trigger` to be stored.
     * @param replaceExisting
     * If `true`, any `Trigger` existing in
     * the `JobStore` with the same name & group should
     * be over-written.
     * @throws ObjectAlreadyExistsException
     * if a `Trigger` with the same name/group already
     * exists, and replaceExisting is set to false.
     *
     * @see .pauseTriggers
     */
    override fun storeTrigger(newTrigger: OperableTrigger?, replaceExisting: Boolean) {
        TODO("not implemented")
    }

    /**
     * Get the names of all of the `[org.quartz.Trigger]`
     * groups.
     *
     *
     *
     * If there are no known group names, the result should be a zero-length
     * array (not `null`).
     *
     */
    override fun getTriggerGroupNames(): MutableList<String> {
        TODO("not implemented")
    }

    /**
     * Retrieve the given `[org.quartz.Trigger]`.
     *
     * @return The desired `Trigger`, or null if there is no
     * match.
     */
    override fun retrieveTrigger(triggerKey: TriggerKey?): OperableTrigger {
        TODO("not implemented")
    }

    /**
     * Remove (delete) the `[org.quartz.Trigger]` with the
     * given key, and store the new given one - which must be associated
     * with the same job.
     *
     * @param newTrigger
     * The new `Trigger` to be stored.
     *
     * @return `true` if a `Trigger` with the given
     * name & group was found and removed from the store.
     */
    override fun replaceTrigger(triggerKey: TriggerKey?, newTrigger: OperableTrigger?): Boolean {
        TODO("not implemented")
    }

    /**
     * Retrieve the `[org.quartz.JobDetail]` for the given
     * `[org.quartz.Job]`.
     *
     * @return The desired `Job`, or null if there is no match.
     */
    override fun retrieveJob(jobKey: JobKey?): JobDetail {
        TODO("not implemented")
    }

    /**
     * Store the given `[org.quartz.Calendar]`.
     *
     * @param calendar
     * The `Calendar` to be stored.
     * @param replaceExisting
     * If `true`, any `Calendar` existing
     * in the `JobStore` with the same name & group
     * should be over-written.
     * @param updateTriggers
     * If `true`, any `Trigger`s existing
     * in the `JobStore` that reference an existing
     * Calendar with the same name with have their next fire time
     * re-computed with the new `Calendar`.
     * @throws ObjectAlreadyExistsException
     * if a `Calendar` with the same name already
     * exists, and replaceExisting is set to false.
     */
    override fun storeCalendar(name: String?, calendar: Calendar?, replaceExisting: Boolean, updateTriggers: Boolean) {
        TODO("not implemented")
    }

    /**
     * Resume (un-pause) the `[org.quartz.Trigger]` with the
     * given key.
     *
     *
     *
     * If the `Trigger` missed one or more fire-times, then the
     * `Trigger`'s misfire instruction will be applied.
     *
     *
     * @see .pauseTrigger
     */
    override fun resumeTrigger(triggerKey: TriggerKey?) {
        TODO("not implemented")
    }

    /**
     * Whether or not the `JobStore` implementation is clustered.
     */
    override fun isClustered(): Boolean = true

    /**
     * Remove (delete) the `[org.quartz.Job]` with the given
     * key, and any `[org.quartz.Trigger]` s that reference
     * it.
     *
     *
     *
     * If removal of the `Job` results in an empty group, the
     * group should be removed from the `JobStore`'s list of
     * known group names.
     *
     *
     * @return `true` if a `Job` with the given name &
     * group was found and removed from the store.
     */
    override fun removeJob(jobKey: JobKey?): Boolean {
        TODO("not implemented")
    }

    /**
     * Called by the QuartzScheduler to inform the `JobStore` that
     * it should free up all of it's resources because the scheduler is
     * shutting down.
     */
    override fun shutdown() {
        // FIXME
        // TODO("not implemented")
    }

    /**
     * Pause all triggers - equivalent of calling `pauseTriggerGroup(group)`
     * on every group.
     *
     *
     *
     * When `resumeAll()` is called (to un-pause), trigger misfire
     * instructions WILL be applied.
     *
     *
     * @see .resumeAll
     * @see .pauseTriggers
     */
    override fun pauseAll() {
        TODO("not implemented")
    }

    /**
     * Get all of the Triggers that are associated to the given Job.
     *
     *
     *
     * If there are no matches, a zero-length array should be returned.
     *
     */
    override fun getTriggersForJob(jobKey: JobKey?): MutableList<OperableTrigger> {
        TODO("not implemented")
    }

    override fun getPausedTriggerGroups(): MutableSet<String> {
        TODO("not implemented")
    }

    /**
     * Clear (delete!) all scheduling data - all [Job]s, [Trigger]s
     * [Calendar]s.
     *
     * @throws JobPersistenceException
     */
    override fun clearAllSchedulingData() {
        TODO("not implemented")
    }

    /**
     * Resume (un-pause) the `[org.quartz.Job]` with the
     * given key.
     *
     *
     *
     * If any of the `Job`'s`Trigger` s missed one
     * or more fire-times, then the `Trigger`'s misfire
     * instruction will be applied.
     *
     *
     * @see .pauseJob
     */
    override fun resumeJob(jobKey: JobKey?) {
        TODO("not implemented")
    }

    /**
     * Get the names of all of the `[org.quartz.Calendar]` s
     * in the `JobStore`.
     *
     *
     *
     * If there are no Calendars in the given group name, the result should be
     * a zero-length array (not `null`).
     *
     */
    override fun getCalendarNames(): MutableList<String> {
        TODO("not implemented")
    }

    /**
     * Called by the QuartzScheduler to inform the `JobStore` that
     * the scheduler has started.
     */
    override fun schedulerStarted() {
        TODO("not implemented")
    }

    /**
     * Called by the QuartzScheduler before the `JobStore` is
     * used, in order to give the it a chance to initialize.
     */
    override fun initialize(loadHelper: ClassLoadHelper?, signaler: SchedulerSignaler?) {
        if (this.db == null) throw KotlinNullPointerException("lateinit var db not initialized")
        logger.info("QuickQuartz running against ${db.readPgVersion()}")
    }

    /**
     * Get the current state of the identified `[Trigger]`.
     *
     * @see Trigger.TriggerState
     */
    override fun getTriggerState(triggerKey: TriggerKey?): Trigger.TriggerState {
        TODO("not implemented")
    }

    override fun setSchedName(schedName: String?) {
        TODO("not implemented")
    }

    override fun setTablePrefix(tablePrefix: String?) {
        if (tablePrefix != null) this.tablePrefix = tablePrefix
    }
}
