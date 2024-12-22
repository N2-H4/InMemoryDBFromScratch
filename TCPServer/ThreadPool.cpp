#include "ThreadPool.h"

VOID CALLBACK workCallback(PTP_CALLBACK_INSTANCE Instance, PVOID Parameter, PTP_WORK Work)
{
	UNREFERENCED_PARAMETER(Instance);
	UNREFERENCED_PARAMETER(Work);

	ThreadWork* w = (struct ThreadWork*)Parameter;
	w->f(w->arg);
	delete w;
	return;
}

void threadPoolInit(ThreadPool* tp)
{
	PTP_POOL pool = NULL;
	TP_CALLBACK_ENVIRON CallBackEnviron;
	PTP_CLEANUP_GROUP cleanupgroup = NULL;

	InitializeThreadpoolEnvironment(&CallBackEnviron);

	pool = CreateThreadpool(NULL);
	if (pool == NULL)
	{
		_tprintf(_T("CreateThreadpool failed. %u\n"), GetLastError());
		return;
	}

	BOOL bRet = FALSE;
	SetThreadpoolThreadMaximum(pool, 4);
	bRet = SetThreadpoolThreadMinimum(pool, 1);

	if (bRet == FALSE)
	{
		_tprintf(_T("SetThreadpoolThreadMinimum failed. %u\n"),GetLastError());
		CloseThreadpool(pool);
		return;
	}

	cleanupgroup = CreateThreadpoolCleanupGroup();
	
	if (cleanupgroup == NULL)
	{
		_tprintf(_T("CreateThreadpoolCleanupGroup failed. %u\n"), GetLastError());
		CloseThreadpool(pool);
		return;
	}

	SetThreadpoolCallbackPool(&CallBackEnviron, pool);
	SetThreadpoolCallbackCleanupGroup(&CallBackEnviron, cleanupgroup, NULL);
	
	tp->callback_env = CallBackEnviron;
	tp->thread_pool = pool;
	tp->cleanup_group = cleanupgroup;
}

int threadPoolSubmitWork(ThreadPool* tp, void(*f)(void*), void* arg)
{
	PTP_WORK work = NULL;
	PTP_WORK_CALLBACK workcallback = workCallback;

	ThreadWork* tw = new ThreadWork();
	tw->f = f;
	tw->arg = arg;
	work = CreateThreadpoolWork(workcallback, tw, &tp->callback_env);

	if (work == NULL)
	{
		_tprintf(_T("CreateThreadpoolWork failed. %u\n"), GetLastError());
		CloseThreadpoolCleanupGroup(tp->cleanup_group);
		CloseThreadpool(tp->thread_pool);
		return -1;
	}

	SubmitThreadpoolWork(work);
	return 0;
}

void threadPoolCleanup(ThreadPool* tp)
{
	CloseThreadpoolCleanupGroupMembers(tp->cleanup_group, FALSE, NULL);
	CloseThreadpoolCleanupGroup(tp->cleanup_group);
	CloseThreadpool(tp->thread_pool);
}