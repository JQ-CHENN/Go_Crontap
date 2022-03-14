package parma

const (
	JOBS_SAVE_DIR = "/cron/jobs/"
	JOBS_KILL_DIR = "/cron/kill/"
	JOBS_LOCK_DIR = "/cron/lock/"
	JOBS_WORKERS_DIR = "/cron/workers/"

	SAVE_SUCCESS = "save success"
	SAVE_FAILED = "save failed"

	DEL_SUCCESS = "delete success"
	DEL_FAILED = "delete failed"

	GET_JOBS_SUCCESS = "get jobs success"
	GET_JOBS_FAILED = "get jobs falied"
	GET_WORKERS_SUCCESS = "get workers success"
	GET_WORKERS_FAILED = "get workers success falied"

	KILL_JOB_SUCCESS = "kill job success"
	KILL_JOB_FAILED = "kill job failed"

	EVENT_SAVE = 1
	EVENT_DEL = 2
	EVENT_KILL = 3

	ERR_LOCK_NEED = "lock Need"
	ERR_NO_LOCAL_IP_FOUND = "ERR_NO_LOCAL_IP_FOUND"

	LOG_NOT_OK = "log list not OK"
	LOG_OK = "log find OK"
)


