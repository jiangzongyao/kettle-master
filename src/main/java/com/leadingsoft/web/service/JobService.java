package com.leadingsoft.web.service;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.beetl.sql.core.ClasspathLoader;
import org.beetl.sql.core.ConnectionSource;
import org.beetl.sql.core.ConnectionSourceHelper;
import org.beetl.sql.core.DSTransactionManager;
import org.beetl.sql.core.Interceptor;
import org.beetl.sql.core.SQLLoader;
import org.beetl.sql.core.SQLManager;
import org.beetl.sql.core.UnderlinedNameConversion;
import org.beetl.sql.core.db.DBStyle;
import org.beetl.sql.core.db.KeyHolder;
import org.beetl.sql.core.db.MySqlStyle;
import org.beetl.sql.ext.DebugInterceptor;
import org.pentaho.di.core.ProgressNullMonitorListener;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingBuffer;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.leadingsoft.common.exception.SeviceException;
import com.leadingsoft.common.kettle.repository.RepositoryUtil;
import com.leadingsoft.common.toolkit.Constant;
import com.leadingsoft.core.dto.BootTablePage;
import com.leadingsoft.core.mapper.KJobDao;
import com.leadingsoft.core.mapper.KJobMonitorDao;
import com.leadingsoft.core.mapper.KQuartzDao;
import com.leadingsoft.core.mapper.KRepositoryDao;
import com.leadingsoft.core.model.KJob;
import com.leadingsoft.core.model.KJobMonitor;
import com.leadingsoft.core.model.KJobRecord;
import com.leadingsoft.core.model.KQuartz;
import com.leadingsoft.core.model.KRepository;
import com.leadingsoft.web.quartz.JobQuartz;
import com.leadingsoft.web.quartz.QuartzManager;
import com.leadingsoft.web.quartz.model.DBConnectionModel;
import com.leadingsoft.web.utils.CommonUtils;

@Service
public class JobService {

	@Autowired
	private KJobDao kJobDao;

	@Autowired
	private KQuartzDao kQuartzDao;

	@Autowired
	private KRepositoryDao KRepositoryDao;

	@Autowired
	private KJobMonitorDao kJobMonitorDao;

	@Value("${kettle.log.file.path}")
	private String kettleLogFilePath;

	@Value("${kettle.file.repository}")
	private String kettleFileRepository;

	@Value("${jdbc.driver}")
	private String jdbcDriver;

	@Value("${jdbc.url}")
	private String jdbcUrl;

	@Value("${jdbc.username}")
	private String jdbcUsername;

	@Value("${jdbc.password}")
	private String jdbcPassword;

	/**
	 * @Title getList
	 * @Description 获取列表
	 * @param start
	 *            其实行数
	 * @param size
	 *            获取数据的条数
	 * @param uId
	 *            用户ID
	 * @return
	 * @return BootTablePage
	 */
	public BootTablePage getList(Integer start, Integer size, Integer uId) {
		KJob template = new KJob();
		template.setAddUser(uId);
		template.setDelFlag(1);
		List<KJob> kJobList = kJobDao.template(template, start, size);
		Long allCount = kJobDao.templateCount(template);
		BootTablePage bootTablePage = new BootTablePage();
		bootTablePage.setRows(kJobList);
		bootTablePage.setTotal(allCount);
		return bootTablePage;
	}

	/**
	 * @Title getList
	 * @Description 获取列表
	 * @param uId
	 *            用户ID
	 * @return
	 * @return List<KJob>
	 */
	public List<KJob> getList(Integer uId) {
		KJob template = new KJob();
		template.setAddUser(uId);
		template.setDelFlag(1);
		return kJobDao.template(template);
	}

	/**
	 * @Title getList
	 * @Description 获取列表
	 * @param uId
	 *            用户ID
	 * @return
	 * @return List<KJob>
	 */
	public List<KJob> getList() {
		KJob template = new KJob();
		template.setJobStatus(1);
		template.setDelFlag(1);
		return kJobDao.template(template);
	}

	/**
	 * @Title delete
	 * @Description 删除作业
	 * @param jobId
	 *            作业ID
	 * @return void
	 */
	public void delete(Integer jobId) {
		KJob kJob = kJobDao.unique(jobId);
		kJob.setDelFlag(0);
		kJobDao.updateById(kJob);
	}

	/**
	 * @Title check
	 * @Description 检查当前作业是否可以插入到数据库
	 * @param repositoryId
	 *            资源库ID
	 * @param jobPath
	 *            作业路径
	 * @param uId
	 *            用户ID
	 * @return
	 * @return boolean
	 */
	public boolean check(Integer repositoryId, String jobPath, Integer uId) {
		KJob template = new KJob();
		template.setDelFlag(1);
		template.setAddUser(uId);
		template.setJobRepositoryId(repositoryId);
		template.setJobPath(jobPath);
		List<KJob> kJobList = kJobDao.template(template);
		if (null != kJobList && kJobList.size() > 0) {
			return false;
		} else {
			return true;
		}
	}

	/**
	 * @Title check
	 * @Description 检查当前作业是否可以插入到数据库
	 * @param repositoryId
	 *            资源库ID
	 * @param jobPath
	 *            作业路径
	 * @param uId
	 *            用户ID
	 * @return
	 * @return Kjob
	 */
	public KJob getOneKJob(Integer repositoryId, String jobPath, Integer uId) {
		KJob template = new KJob();
		template.setDelFlag(1);
		template.setAddUser(uId);
		template.setJobRepositoryId(repositoryId);
		template.setJobPath(jobPath);
		List<KJob> kJobList = kJobDao.template(template);
		if (null != kJobList && kJobList.size() > 0) {
			return kJobList.get(0);
		} else {
			return null;
		}
	}

	/**
	 * @Title saveFile
	 * @Description 保存上传的作业文件
	 * @param uId
	 *            用户ID
	 * @param jobFile
	 *            上传的作业文件
	 * @return
	 * @throws IOException
	 * @return String
	 */
	public String saveFile(Integer uId, MultipartFile jobFile) throws IOException {
		return CommonUtils.saveFile(uId, kettleFileRepository, jobFile);
	}

	/**
	 * @Title insert
	 * @Description 插入作业到数据库
	 * @param kJob
	 *            作业信息
	 * @param uId
	 *            用户ID
	 * @param customerQuarz
	 *            自定义定时策略
	 * @throws SQLException
	 * @return void
	 */
	public void insert(KJob kJob, Integer uId, String customerQuarz) throws SQLException {
		DSTransactionManager.start();
		// 补充添加作业信息
		// 作业基础信息
		kJob.setAddUser(uId);
		kJob.setAddTime(new Date());
		kJob.setEditUser(uId);
		kJob.setEditTime(new Date());
		// 作业是否被删除
		kJob.setDelFlag(1);
		// 作业是否启动
		kJob.setJobStatus(2);
		if (StringUtils.isNotBlank(customerQuarz)) {
			// 插入调度策略
			kJob.setJobQuartz(customerQuarz);
		}
		kJobDao.insert(kJob);
		DSTransactionManager.commit();
	}

	/**
	 * @Title getJob
	 * @Description 获取作业信息
	 * @param jobId
	 *            作业ID
	 * @return
	 * @return KJob
	 */
	public KJob getJob(Integer jobId) {
		return kJobDao.single(jobId);
	}

	/**
	 * @Title update
	 * @Description 更新作业信息
	 * @param kJob
	 *            作业对象
	 * @param customerQuarz
	 *            自定义定时策略
	 * @param uId
	 *            用户ID
	 * @return void
	 */
	public void update(KJob kJob, String customerQuarz, Integer uId) {
		kJob.setJobQuartz(customerQuarz);
		kJobDao.updateTemplateById(kJob);
	}

	/**
	 * @Title start
	 * @Description 启动作业
	 * @param jobId
	 *            作业ID
	 * @throws SeviceException
	 * @return void
	 * symphonyJobType: 1.api触发，2，定时触发，3.依赖触发
	 */
	public void start(Integer jobId,String symphonyJobType) {
		// 获取到作业对象
		KJob kJob = kJobDao.unique(jobId);
		// 获取到定时策略对象
		KQuartz kQuartz = kQuartzDao.unique(kJob.getJobQuartz());
		// 定时策略
		String quartzCron = kQuartz.getQuartzCron();
		// 用户ID
		Integer userId = kJob.getAddUser();
		// 获取调度任务的基础信息
		Map<String, String> quartzBasic = getQuartzBasic(kJob);
		// Quartz执行时的参数
		Map<String, Object> quartzParameter = getQuartzParameter(kJob);
		// 添加监控
		if("2".equals(symphonyJobType)){
			addMonitor(userId, jobId,1);
			kJob.setJobStatus(1);
			// 判断作业执行类型
			if (new Integer(1).equals(kJob.getJobQuartz())) {// 如果是只执行一次
				QuartzManager.addOnceJob(quartzBasic.get("jobName"), quartzBasic.get("jobGroupName"),
						quartzBasic.get("triggerName"), quartzBasic.get("triggerGroupName"), JobQuartz.class,
						quartzParameter);
			} else {// 如果是按照策略执行
					// 添加任务
					QuartzManager.addJob(quartzBasic.get("jobName"), quartzBasic.get("jobGroupName"),
							quartzBasic.get("triggerName"), quartzBasic.get("triggerGroupName"), JobQuartz.class,
							quartzCron, quartzParameter);
			}
		}else{
			addMonitor(userId, jobId,2);
			kJob.setJobStatus(2);
		}
		kJobDao.updateTemplateById(kJob);
	}

	/**
	 * @Title stop
	 * @Description 停止作业
	 * @param jobId
	 *            作业ID
	 * @throws SeviceException
	 * @return void
	 */
	public void stop(Integer jobId) {
		// 获取到作业对象
		KJob kJob = kJobDao.unique(jobId);
		// 用户ID
		Integer userId = kJob.getAddUser();
		// 获取调度任务的基础信息
		Map<String, String> quartzBasic = getQuartzBasic(kJob);
		if (new Integer(1).equals(kJob.getJobQuartz())) {// 如果是只执行一次
			// 一次性执行任务，不允许手动停止

		} else {// 如果是按照策略执行
				// 移除任务
			QuartzManager.removeJob(quartzBasic.get("jobName"), quartzBasic.get("jobGroupName"),
					quartzBasic.get("triggerName"), quartzBasic.get("triggerGroupName"));
		}
		// 移除监控
		removeMonitor(userId, jobId);
		kJob.setJobStatus(2);
		kJobDao.updateTemplateById(kJob);
	}

	/**
	 * @Title getQuartzBasic
	 * @Description 获取任务调度的基础信息
	 * @param kTrans
	 *            转换对象
	 * @return
	 * @return Map<String, String> 任务调度的基础信息
	 */
	private Map<String, String> getQuartzBasic(KJob kJob) {
		Integer userId = kJob.getAddUser();
		Integer transRepositoryId = kJob.getJobRepositoryId();
		String jobPath = kJob.getJobPath();
		Map<String, String> quartzBasic = new HashMap<String, String>();
		// 拼接Quartz的任务名称
		StringBuilder jobName = new StringBuilder();
		jobName.append(Constant.JOB_PREFIX).append(Constant.QUARTZ_SEPARATE).append(transRepositoryId)
				.append(Constant.QUARTZ_SEPARATE).append(jobPath);
		// 拼接Quartz的任务组名称
		StringBuilder jobGroupName = new StringBuilder();
		jobGroupName.append(Constant.JOB_GROUP_PREFIX).append(Constant.QUARTZ_SEPARATE).append(userId)
				.append(Constant.QUARTZ_SEPARATE).append(transRepositoryId).append(Constant.QUARTZ_SEPARATE)
				.append(jobPath);
		// 拼接Quartz的触发器名称
		String triggerName = StringUtils.replace(jobName.toString(), Constant.JOB_PREFIX, Constant.TRIGGER_PREFIX);
		// 拼接Quartz的触发器组名称
		String triggerGroupName = StringUtils.replace(jobGroupName.toString(), Constant.JOB_GROUP_PREFIX,
				Constant.TRIGGER_GROUP_PREFIX);
		quartzBasic.put("jobName", jobName.toString());
		quartzBasic.put("jobGroupName", jobGroupName.toString());
		quartzBasic.put("triggerName", triggerName);
		quartzBasic.put("triggerGroupName", triggerGroupName);
		return quartzBasic;
	}

	/**
	 * @Title getQuartzParameter
	 * @Description 获取任务调度的参数
	 * @param kTrans
	 *            转换对象
	 * @return
	 * @return Map<String, Object>
	 */
	private Map<String, Object> getQuartzParameter(KJob kJob) {
		// Quartz执行时的参数
		Map<String, Object> parameter = new HashMap<String, Object>();
		// 资源库对象
		Integer transRepositoryId = kJob.getJobRepositoryId();
		KRepository kRepository = null;
		if (transRepositoryId != null) {// 这里是判断是否为资源库中的转换还是文件类型的转换的关键点
			kRepository = KRepositoryDao.single(transRepositoryId);
		}
		// 资源库对象
		parameter.put(Constant.REPOSITORYOBJECT, kRepository);
		// 数据库连接对象
		parameter.put(Constant.DBCONNECTIONOBJECT,
				new DBConnectionModel(jdbcDriver, jdbcUrl, jdbcUsername, jdbcPassword));
		// 转换ID
		parameter.put(Constant.JOBID, kJob.getJobId());
		String jobPath = kJob.getJobPath();
		if (jobPath.contains("/")) {
			int lastIndexOf = StringUtils.lastIndexOf(jobPath, "/");
			String path = jobPath.substring(0, lastIndexOf);
			// 转换在资源库中的路径
			parameter.put(Constant.JOBPATH, StringUtils.isEmpty(path) ? "/" : path);
			// 转换名称
			parameter.put(Constant.JOBNAME, jobPath.substring(lastIndexOf + 1, jobPath.length()));
		}
		// 用户ID
		parameter.put(Constant.USERID, kJob.getAddUser());
		// 转换日志等级
		parameter.put(Constant.LOGLEVEL, kJob.getJobLogLevel());
		// 转换日志的保存位置
		parameter.put(Constant.LOGFILEPATH, kettleLogFilePath);
		return parameter;
	}

	/**
	 * @Title addMonitor
	 * @Description 添加监控
	 * @param userId
	 *            用户ID
	 * @param transId
	 *            转换ID
	 * @return void
	 *  status 1,启动。2，禁止
	 */
	private void addMonitor(Integer userId, Integer jobId,Integer status) {
		KJobMonitor template = new KJobMonitor();
		template.setAddUser(userId);
		template.setMonitorJob(jobId);
		KJobMonitor templateOne = kJobMonitorDao.templateOne(template);
		if (null != templateOne) {
			templateOne.setMonitorStatus(status);
			StringBuilder runStatusBuilder = new StringBuilder();
			runStatusBuilder.append(templateOne.getRunStatus()).append(",").append(new Date().getTime())
					.append(Constant.RUNSTATUS_SEPARATE);
			templateOne.setRunStatus(runStatusBuilder.toString());
			kJobMonitorDao.updateTemplateById(templateOne);
		} else {
			KJobMonitor kJobMonitor = new KJobMonitor();
			kJobMonitor.setMonitorJob(jobId);
			kJobMonitor.setAddUser(userId);
			kJobMonitor.setMonitorSuccess(0);
			kJobMonitor.setMonitorFail(0);
			StringBuilder runStatusBuilder = new StringBuilder();
			runStatusBuilder.append(new Date().getTime()).append(Constant.RUNSTATUS_SEPARATE);
			kJobMonitor.setRunStatus(runStatusBuilder.toString());
			kJobMonitor.setMonitorStatus(1);
			kJobMonitorDao.insert(kJobMonitor);
		}
	}

	/**
	 * @Title removeMonitor
	 * @Description 移除监控
	 * @param userId
	 *            用户ID
	 * @param transId
	 *            转换ID
	 * @return void
	 */
	private void removeMonitor(Integer userId, Integer jobId) {
		KJobMonitor template = new KJobMonitor();
		template.setAddUser(userId);
		template.setMonitorJob(jobId);
		KJobMonitor templateOne = kJobMonitorDao.templateOne(template);
		templateOne.setMonitorStatus(2);
		StringBuilder runStatusBuilder = new StringBuilder();
		runStatusBuilder.append(templateOne.getRunStatus()).append(new Date().getTime());
		templateOne.setRunStatus(runStatusBuilder.toString());
		kJobMonitorDao.updateTemplateById(templateOne);
	}

	public void startJob(String jobPath) {
		// 获取到作业对象
		KJob kJob = getOneKJob(3,jobPath,1);
		// Quartz执行时的参数
		Map<String, Object> quartzParameter = getQuartzParameter(kJob);
		Object KRepositoryObject = quartzParameter.get(Constant.REPOSITORYOBJECT);
		Object DbConnectionObject = quartzParameter.get(Constant.DBCONNECTIONOBJECT);
		String jobIdstr = String.valueOf(quartzParameter.get(Constant.JOBID));
		String jobPathstr = String.valueOf(quartzParameter.get(Constant.JOBPATH));
		String jobName = String.valueOf(quartzParameter.get(Constant.JOBNAME));
		String userIdstr = String.valueOf(quartzParameter.get(Constant.USERID));
		String logLevel = String.valueOf(quartzParameter.get(Constant.LOGLEVEL));
		String logFilePath = String.valueOf(quartzParameter.get(Constant.LOGFILEPATH));
			try {
				runRepositoryJob(KRepositoryObject,  DbConnectionObject,  jobIdstr, jobPathstr,  jobName,  userIdstr,  logLevel,  logFilePath);
			} catch (KettleException e) {
				e.printStackTrace();
			}
	}
	/**
	 * @Title runRepositoryJob
	 * @Description 运行资源库中的作业
	 * @param KRepositoryObject
	 *            数据库连接对象
	 * @param KRepositoryObject
	 *            资源库对象
	 * @param jobId
	 *            作业ID
	 * @param jobPath
	 *            作业在资源库中的路径信息
	 * @param jobName
	 *            作业名称
	 * @param userId
	 *            作业归属者ID
	 * @param logLevel
	 *            作业的日志等级
	 * @param logFilePath
	 *            作业日志保存的根路径
	 * @throws KettleException
	 * @return void
	 */
	public void runRepositoryJob(Object KRepositoryObject, Object DbConnectionObject, String jobId,
			String jobPath, String jobName, String userId, String logLevel, String logFilePath) throws KettleException {
		KRepository kRepository = (KRepository) KRepositoryObject;
		KettleDatabaseRepository kettleDatabaseRepository = RepositoryUtil.connectionRepository(kRepository);
		if (null != kettleDatabaseRepository) {
			RepositoryDirectoryInterface directory = kettleDatabaseRepository.loadRepositoryDirectoryTree()
					.findDirectory(jobPath);
			JobMeta jobMeta = kettleDatabaseRepository.loadJob(jobName, directory, new ProgressNullMonitorListener(),
					null);
			org.pentaho.di.job.Job job = new org.pentaho.di.job.Job(kettleDatabaseRepository, jobMeta);
			job.setDaemon(true);
			job.setLogLevel(LogLevel.ROWLEVEL);
			if (StringUtils.isNotEmpty(logLevel)) {
				job.setLogLevel(Constant.logger(logLevel));
			}
			String exception = null;
			Integer recordStatus = 1;
			Date jobStartDate = null;
			Date jobStopDate = null;
			String logText = null;
			try {
				jobStartDate = new Date();
				job.run();
				job.waitUntilFinished();
				jobStopDate = new Date();
			} catch (Exception e) {
				exception = e.getMessage();
				recordStatus = 2;
			} finally {
				if (job.isFinished()) {
					if (job.getErrors() > 0
							&& (null == job.getResult().getLogText() || "".equals(job.getResult().getLogText()))) {
						logText = exception;
					}
					// 写入作业执行结果
					StringBuilder allLogFilePath = new StringBuilder();
					allLogFilePath.append(logFilePath).append("/").append(userId).append("/")
							.append(StringUtils.remove(jobPath, "/")).append("@").append(jobName).append("-log")
							.append("/").append(new Date().getTime()).append(".").append("txt");
					String logChannelId = job.getLogChannelId();
					LoggingBuffer appender = KettleLogStore.getAppender();
					logText = appender.getBuffer(logChannelId, true).toString();
					try {
						KJobRecord kJobRecord = new KJobRecord();
						kJobRecord.setRecordJob(Integer.parseInt(jobId));
						kJobRecord.setAddUser(Integer.parseInt(userId));
						kJobRecord.setLogFilePath(allLogFilePath.toString());
						kJobRecord.setRecordStatus(recordStatus);
						kJobRecord.setStartTime(jobStartDate);
						kJobRecord.setStopTime(jobStopDate);
						writeToDBAndFile(DbConnectionObject, kJobRecord, logText);
					} catch (IOException | SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	/**
	 * @Title writeToDBAndFile
	 * @Description 保存作业运行日志信息到文件和数据库
	 * @param DbConnectionObject 数据库连接对象
	 * @param kJobRecord 作业记录信息
	 * @param logText 日志信息
	 * @throws IOException
	 * @throws SQLException
	 * @return void
	 */
	private void writeToDBAndFile(Object DbConnectionObject, KJobRecord kJobRecord, String logText)
			throws IOException, SQLException {
		// 将日志信息写入文件
		FileUtils.writeStringToFile(new File(kJobRecord.getLogFilePath()), logText, Constant.DEFAULT_ENCODING, false);
		// 写入转换运行记录到数据库
		DBConnectionModel DBConnectionModel = (DBConnectionModel) DbConnectionObject;
		ConnectionSource source = ConnectionSourceHelper.getSimple(DBConnectionModel.getConnectionDriveClassName(), 
				DBConnectionModel.getConnectionUrl(), DBConnectionModel.getConnectionUser(), DBConnectionModel.getConnectionPassword());
		DBStyle mysql = new MySqlStyle();
		SQLLoader loader = new ClasspathLoader("/");
		UnderlinedNameConversion nc = new  UnderlinedNameConversion();
		SQLManager sqlManager = new SQLManager(mysql, loader, 
				source, nc, new Interceptor[]{new DebugInterceptor()});
		DSTransactionManager.start();
		sqlManager.insert(kJobRecord);
		KJobMonitor template = new KJobMonitor();
		template.setAddUser(kJobRecord.getAddUser());
		KJobMonitor templateOne = sqlManager.templateOne(template);
		if(kJobRecord.getRecordStatus() == 1){// 证明成功
			//成功次数加1
			templateOne.setMonitorSuccess(templateOne.getMonitorSuccess() + 1);
			sqlManager.updateTemplateById(templateOne);
		}else if (kJobRecord.getRecordStatus() == 2){// 证明失败
			//失败次数加1
			templateOne.setMonitorFail(templateOne.getMonitorFail() + 1);
			sqlManager.updateTemplateById(templateOne);
		}
		DSTransactionManager.commit();
	}
}
