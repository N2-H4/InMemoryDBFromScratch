#pragma once
#include <windows.h>
#include <stdio.h>
#include <tchar.h>

struct ThreadPool
{
	TP_CALLBACK_ENVIRON callback_env;
	PTP_POOL thread_pool;
	PTP_CLEANUP_GROUP cleanup_group;
};

struct ThreadWork 
{
	void (*f)(void*) = NULL;
	void* arg = NULL;
};


void threadPoolInit(ThreadPool* tp);
int threadPoolSubmitWork(ThreadPool* tp, void(*f)(void*), void* arg);
void threadPoolCleanup(ThreadPool* tp);