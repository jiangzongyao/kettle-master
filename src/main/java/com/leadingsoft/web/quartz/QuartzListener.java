package com.leadingsoft.web.quartz;

import java.util.Date;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;

public class QuartzListener implements JobListener{

	@Override
	public String getName() {
		return new Date().getTime() + "QuartzListener";
	}
	@Override
	public void jobToBeExecuted(JobExecutionContext context) {
	}
	@Override
	public void jobExecutionVetoed(JobExecutionContext context) {
	}
	@Override
	public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
		String jobName = context.getJobDetail().getKey().getName();
		String jobGroupName = context.getJobDetail().getKey().getGroup();
		String triggerName = context.getTrigger().getKey().getName();
		String triggerGroupName = context.getTrigger().getKey().getGroup();
		//一次性任务，执行完之后需要移除
		QuartzManager.removeJob(jobName, jobGroupName, triggerName, triggerGroupName);
	}
}
