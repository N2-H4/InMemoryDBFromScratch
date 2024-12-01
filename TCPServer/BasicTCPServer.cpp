#include <winsock2.h>
#include <ws2tcpip.h>
#include <stdio.h>
#include <stdint.h>
#include <vector>
#include <string>
#include <map>
#include "ZSet.h"

#pragma comment(lib, "Ws2_32.lib")

#define DEFAULT_PORT "27015"
#define DEFAULT_BUFLEN 512

#define container_of(ptr, T, member) \
    (T *)( (char *)ptr - offsetof(T, member) )

const int k_max_msg = 4096;
const int k_max_args = 1024;

enum 
{
	STATE_REQ = 0,
	STATE_RES = 1,
	STATE_END = 2,
};

enum 
{
	RES_OK = 0,
	RES_ERR = 1,
	RES_NX = 2,
};

enum 
{
	ERR_UNKNOWN = 1,
	ERR_2BIG = 2,
	ERR_TYPE = 3,
	ERR_ARG = 4,
};

enum 
{
	SER_NULL = 0,   //null
	SER_ERR = 1,    //error code and message
	SER_STR = 2,    //string
	SER_INT = 3,    //int
	SER_ARR = 4,    //array
	SER_DBL = 5,	//double
};

enum 
{
	T_STR = 0,
	T_ZSET = 1,
};

struct Entry 
{
	struct Node node;
	std::string key;
	std::string val;
	unsigned int type = 0;
	ZSet* zset = NULL;
};

static struct 
{
	HashMap db;
} g_data;

static std::map<std::string, std::string> g_map;

struct Conn 
{
	SOCKET socket = INVALID_SOCKET;
	unsigned int state = 0;
	int rbuf_size = 0;
	int rbuf_processed = 0;
	char rbuf[4 + k_max_msg];
	int wbuf_size = 0;
	int wbuf_sent = 0;
	char wbuf[4 + k_max_msg];
};

static unsigned long long strHash(const char* data, unsigned long long len)
{
	unsigned long long h = 0x811C9DC5;
	for (unsigned long long i = 0; i < len; i++)
	{
		h = (h + data[i]) * 0x01000193;
	}
	return h;
}

static bool entryEq(Node* lhs, Node* rhs)
{
	struct Entry* le = container_of(lhs, struct Entry, node);
	struct Entry* re = container_of(rhs, struct Entry, node);
	return le->key == re->key;
}

static int readFull(SOCKET socket, char* buf, int n)
{
	while (n > 0)
	{
		int iResult = recv(socket, buf, n, 0);
		if (iResult <= 0)
			return -1;
		n -= iResult;
		buf += iResult;
	}
	return 0;
}

static int writeAll(SOCKET socket, const char* buf, int n)
{
	while (n > 0)
	{
		int iResult = send(socket, buf, n, 0);
		if (iResult <= 0)
			return -1;
		n -= iResult;
		buf += iResult;
	}
	return 0;
}

static void connPut(std::vector<Conn*>& conns, struct Conn* conn) 
{
	if (conns.size() <= (size_t)conn->socket) {
		conns.resize(conn->socket + 1);
	}
	conns[conn->socket] = conn;
}

static int acceptNewConn(std::vector<Conn*>& conns, SOCKET socket) 
{
	struct sockaddr_in client_addr = {};
	socklen_t socklen = sizeof(client_addr);
	SOCKET accepted_socket = accept(socket, (struct sockaddr*)&client_addr, &socklen);
	if (accepted_socket == INVALID_SOCKET) 
	{
		printf("Error at accepting client socket: %ld\n", WSAGetLastError());
		WSACleanup();
		return -1; 
	}

	unsigned long mode = 1;
	int iResult;
	iResult = ioctlsocket(accepted_socket, FIONBIO, &mode);
	if (iResult == SOCKET_ERROR) 
	{
		printf("Error at setting nonblocking io: %ld\n", WSAGetLastError());
		closesocket(accepted_socket);
		WSACleanup();
		return -1;
	}

	struct Conn* conn = (struct Conn*)malloc(sizeof(struct Conn));
	if (!conn) 
	{
		closesocket(accepted_socket);
		return -1;
	}
	conn->socket = accepted_socket;
	conn->state = STATE_REQ;
	conn->rbuf_size = 0;
	conn->rbuf_processed = 0;
	conn->wbuf_size = 0;
	conn->wbuf_sent = 0;
	connPut(conns, conn);
	return 0;
}

static void outErr(std::string& out, int code, const std::string& msg) 
{
	out.push_back(SER_ERR);
	out.append((char*)&code, 4);
	unsigned int len = (unsigned int)msg.size();
	out.append((char*)&len, 4);
	out.append(msg);
}

static void outNull(std::string& out) 
{
	out.push_back(SER_NULL);
}

static void outStr(std::string& out, const std::string& val) 
{
	out.push_back(SER_STR);
	unsigned int len = (unsigned int)val.size();
	out.append((char*)&len, 4);
	out.append(val);
}

static void outStr(std::string& out, const char* s, size_t size) 
{
	out.push_back(SER_STR);
	unsigned int len = (unsigned int)size;
	out.append((char*)&len, 4);
	out.append(s, len);
}

static void outInt(std::string& out, long long val) 
{
	out.push_back(SER_INT);
	out.append((char*)&val, 8);
}

static void outArr(std::string& out, unsigned int n) 
{
	out.push_back(SER_ARR);
	out.append((char*)&n, 4);
}

static void* beginArr(std::string& out) 
{
	out.push_back(SER_ARR);
	out.append("\0\0\0\0", 4);
	return (void*)(out.size() - 4);
}

static void endArr(std::string& out, void* ctx, unsigned int n) 
{
	size_t pos = (size_t)ctx;
	if (out[pos - 1] != SER_ARR)
	{
		out.append("arr err");
	}
	memcpy(&out[pos], &n, 4);
}

static void outDbl(std::string& out, double val) 
{
	out.push_back(SER_DBL);
	out.append((char*)&val, 8);
}

static bool str2dbl(const std::string& s, double& out) 
{
	char* endp = NULL;
	out = strtod(s.c_str(), &endp);
	return endp == s.c_str() + s.size() && !isnan(out);
}

static bool str2int(const std::string& s, long long& out) 
{
	char* endp = NULL;
	out = strtoll(s.c_str(), &endp, 10);
	return endp == s.c_str() + s.size();
}

static bool expectZSet(std::string& out, std::string& s, Entry** ent) 
{
	Entry key;
	key.key.swap(s);
	key.node.hash_code = strHash(key.key.data(), key.key.size());
	Node* hnode = hashMapLookup(&g_data.db, &key.node, &entryEq);
	if (!hnode) 
	{
		outNull(out);
		return false;
	}

	*ent = container_of(hnode, Entry, node);
	if ((*ent)->type != T_ZSET) 
	{
		outErr(out, ERR_TYPE, "expect zset");
		return false;
	}
	return true;
}

static void hScan(HashTable* tab, void (*f)(Node*, void*), void* arg) 
{
	if (tab->size == 0) 
	{
		return;
	}
	for (unsigned long long i = 0; i < tab->mask + 1; ++i) 
{
		Node* node = tab->tab[i];
		while (node) 
		{
			f(node, arg);
			node = node->next;
		}
	}
}

static void cbScan(Node* node, void* arg) 
{
	std::string& out = *(std::string*)arg;
	struct Entry* e = container_of(node, Entry, node);
	outStr(out, e->key);
}

static void doKeys(std::vector<std::string>& cmd, std::string& out) 
{
	(void)cmd;
	outArr(out, (unsigned int)hashMapSize(&g_data.db));
	hScan(&g_data.db.ht1, &cbScan, &out);
	hScan(&g_data.db.ht2, &cbScan, &out);
}

static void doGet(std::vector<std::string>& cmd, std::string& out)
{
	Entry entry;
	entry.key.swap(cmd[1]);
	entry.node.hash_code = strHash(entry.key.data(), entry.key.size());

	Node* n = hashMapLookup(&g_data.db, &entry.node, &entryEq);
	if (!n) 
	{
		return outNull(out);
	}

	struct Entry* e = container_of(n, struct Entry, node);
	if (e->type != T_STR) 
	{
		return outErr(out, ERR_TYPE, "expect string type");
	}
	return outStr(out, e->val);
}

static void doSet(std::vector<std::string>& cmd, std::string& out)
{;
	Entry entry;
	entry.key.swap(cmd[1]);
	entry.node.hash_code = strHash(entry.key.data(), entry.key.size());

	Node* node = hashMapLookup(&g_data.db, &entry.node, &entryEq);
	if (node) 
	{
		struct Entry* e = container_of(node, struct Entry, node);
		if (e->type != T_STR) 
		{
			return outErr(out, ERR_TYPE, "expect string type");
		}
		e->val.swap(cmd[2]);
	}
	else 
	{
		Entry* ent = new Entry();
		ent->key.swap(entry.key);
		ent->node.hash_code = entry.node.hash_code;
		ent->val.swap(cmd[2]);
		hashMapInsert(&g_data.db, &ent->node);
	}
	return outNull(out);
}

static void doZAdd(std::vector<std::string>& cmd, std::string& out) 
{
	double score = 0;
	if (!str2dbl(cmd[2], score)) 
	{
		return outErr(out, ERR_ARG, "expect fp number");
	}

	Entry key;
	key.key.swap(cmd[1]);
	key.node.hash_code = strHash(key.key.data(), key.key.size());
	Node* hnode = hashMapLookup(&g_data.db, &key.node, &entryEq);

	Entry* ent = NULL;
	if (!hnode) 
	{
		ent = new Entry();
		ent->key.swap(key.key);
		ent->node.hash_code = key.node.hash_code;
		ent->type = T_ZSET;
		ent->zset = new ZSet();
		hashMapInsert(&g_data.db, &ent->node);
	}
	else 
	{
		ent = container_of(hnode, Entry, node);
		if (ent->type != T_ZSET) 
		{
			return outErr(out, ERR_TYPE, "expect zset");
		}
	}

	const std::string& name = cmd[3];
	bool added = zSetAdd(ent->zset, name.data(), name.size(), score);
	return outInt(out, (int)added);
}

static void entryDel(Entry* ent) 
{
	switch (ent->type) 
	{
	case T_ZSET:
		zSetDispose(ent->zset);
		delete ent->zset;
		break;
	}
	delete ent;
}

static void doDel(std::vector<std::string>& cmd, std::string& out)
{
	Entry entry;
	entry.key.swap(cmd[1]);
	entry.node.hash_code = strHash(entry.key.data(), entry.key.size());

	Node* node = hashMapPop(&g_data.db, &entry.node, &entryEq);
	if (node) 
	{
		delete container_of(node, Entry, node);
	}
	return outInt(out, node ? 1 : 0);
}

static void doZQuery(std::vector<std::string>& cmd, std::string& out) 
{
	double score = 0;
	if (!str2dbl(cmd[2], score)) 
	{
		return outErr(out, ERR_ARG, "expect fp number");
	}
	const std::string& name = cmd[3];
	long long offset = 0;
	long long limit = 0;
	if (!str2int(cmd[4], offset)) 
	{
		return outErr(out, ERR_ARG, "expect int");
	}
	if (!str2int(cmd[5], limit)) 
	{
		return outErr(out, ERR_ARG, "expect int");
	}

	Entry* ent = NULL;
	if (!expectZSet(out, cmd[1], &ent)) 
	{
		if (out[0] == SER_NULL) 
		{
			out.clear();
			outArr(out, 0);
		}
		return;
	}

	if (limit <= 0) 
	{
		return outArr(out, 0);
	}
	ZNode* znode = zSetQuery(ent->zset, score, name.data(), name.size());
	znode = zNodeOffset(znode, offset);

	void* arr = beginArr(out);
	unsigned int n = 0;
	while (znode && (long long)n < limit) 
	{
		outStr(out, znode->name, znode->len);
		outDbl(out, znode->score);
		znode = zNodeOffset(znode, +1);
		n += 2;
	}
	endArr(out, arr, n);
}
static void doZRem(std::vector<std::string>& cmd, std::string& out) 
{
	Entry* ent = NULL;
	if (!expectZSet(out, cmd[1], &ent)) 
	{
		return;
	}

	const std::string& name = cmd[2];
	ZNode* znode = zSetPop(ent->zset, name.data(), name.size());
	if (znode) 
	{
		zNodeDel(znode);
	}
	return outInt(out, znode ? 1 : 0);
}

static void doZScore(std::vector<std::string>& cmd, std::string& out) 
{
	Entry* ent = NULL;
	if (!expectZSet(out, cmd[1], &ent)) 
	{
		return;
	}

	const std::string& name = cmd[2];
	ZNode* znode = zSetLookup(ent->zset, name.data(), name.size());
	return znode ? outDbl(out, znode->score) : outNull(out);
}


static bool cmdIs(const std::string& word, const char* cmd) 
{
	return strcmp(word.c_str(), cmd) == 0;
}

static int parseReq(const char* data, size_t len, std::vector<std::string>& out)
{
	if (len < 4) 
	{
		return -1;
	}
	unsigned int n = 0;
	memcpy(&n, &data[0], 4);
	if (n > k_max_args) 
	{
		return -1;
	}

	size_t pos = 4;
	while (n--) 
	{
		if (pos + 4 > len) 
		{
			return -1;
		}
		unsigned int sz = 0;
		memcpy(&sz, &data[pos], 4);
		if (pos + 4 + sz > len) 
		{
			return -1;
		}
		out.push_back(std::string(&data[pos + 4], sz));
		pos += 4 + sz;
	}

	if (pos != len) 
	{
		return -1;
	}

	return 0;
}

//static int doRequest(const char* req, unsigned int reqlen, unsigned int* rescode, char* res, unsigned int* reslen)
//{
//	std::vector<std::string> cmd;
//	if (0 != parseReq(req, reqlen, cmd)) 
//	{
//		printf("bad request\n");
//		return -1;
//	}
//	if (cmd.size() == 2 && cmdIs(cmd[0], "get")) 
//	{
//		*rescode = doGet(cmd, res, reslen);
//	}
//	else if (cmd.size() == 3 && cmdIs(cmd[0], "set")) 
//	{
//		*rescode = doSet(cmd, res, reslen);
//	}
//	else if (cmd.size() == 2 && cmdIs(cmd[0], "del")) 
//	{
//		*rescode = doDel(cmd, res, reslen);
//	}
//	else 
//	{
//		*rescode = RES_ERR;
//		const char* msg = "Unknown cmd";
//		strcpy_s(res, _TRUNCATE, msg);
//		*reslen = strlen(msg);
//		return 0;
//	}
//	return 0;
//}

static void doRequest(std::vector<std::string>& cmd, std::string& out) 
{
	if (cmd.size() == 1 && cmdIs(cmd[0], "keys")) 
	{
		doKeys(cmd, out);
	}
	else if (cmd.size() == 2 && cmdIs(cmd[0], "get")) 
	{
		doGet(cmd, out);
	}
	else if (cmd.size() == 3 && cmdIs(cmd[0], "set")) 
	{
		doSet(cmd, out);
	}
	else if (cmd.size() == 2 && cmdIs(cmd[0], "del")) 
	{
		doDel(cmd, out);
	}
	else if (cmd.size() == 4 && cmdIs(cmd[0], "zadd")) 
	{
		doZAdd(cmd, out);
	}
	else if (cmd.size() == 3 && cmdIs(cmd[0], "zrem")) 
	{
		doZRem(cmd, out);
	}
	else if (cmd.size() == 3 && cmdIs(cmd[0], "zscore")) 
	{
		doZScore(cmd, out);
	}
	else if (cmd.size() == 6 && cmdIs(cmd[0], "zquery")) 
	{
		doZQuery(cmd, out);
	}
	else 
	{
		outErr(out, ERR_UNKNOWN, "Unknown cmd");
	}
}

static bool tryFlushBuffer(Conn* conn)
{
	int rv = 0;
	size_t remain = conn->wbuf_size - conn->wbuf_sent;
	rv = send(conn->socket, &conn->wbuf[conn->wbuf_sent], remain, 0);
	if (rv == SOCKET_ERROR && WSAGetLastError() == WSAEWOULDBLOCK)
	{
		return false;
	}
	else if (rv == SOCKET_ERROR)
	{
		printf("Error send()\n");
		conn->state = STATE_END;
		return false;
	}
	conn->wbuf_sent += (size_t)rv;
	if (conn->wbuf_sent > conn->wbuf_size)
	{
		printf("Sent more data than write buffer contains\n");
		conn->state = STATE_END;
		return false;
	}
	if (conn->wbuf_sent == conn->wbuf_size)
	{
		conn->state = STATE_REQ;
		conn->wbuf_sent = 0;
		conn->wbuf_size = 0;
		return false;
	}

	return true;
}

static void stateRes(Conn* conn)
{
	while (tryFlushBuffer(conn)) {}
}

static bool tryOneRequest(Conn* conn) 
{
	if (conn->rbuf_size < 4)
		return false;

	if (conn->rbuf_processed >= conn->rbuf_size)
		return false;

	unsigned int len = 0;
	memcpy(&len, &conn->rbuf[conn->rbuf_processed], 4);
	if (len > k_max_msg) 
	{
		printf("Message too long\n");
		conn->state = STATE_END;
		return false;
	}
	if (4 + len > (conn->rbuf_size - conn->rbuf_processed))
		return false;

	std::vector<std::string> cmd;
	if (0 != parseReq(&conn->rbuf[4], len, cmd)) 
	{
		printf("bad req\n");
		conn->state = STATE_END;
		return false;
	}

	std::string out;
	doRequest(cmd, out);

	if (4 + out.size() > k_max_msg) 
	{
		out.clear();
		outErr(out, ERR_2BIG, "response is too big");
	}

	unsigned int wlen = (unsigned int)out.size();

	memcpy(&conn->wbuf[0], &wlen, 4);
	memcpy(&conn->wbuf[4], out.data(), out.size());
	conn->wbuf_size = 4 + wlen;

	conn->rbuf_processed += 4 + len;

	conn->state = STATE_RES;
	stateRes(conn);

	return (conn->state == STATE_REQ);
}

static bool tryFillBuffer(Conn* conn) 
{
	if (conn->rbuf_size >= sizeof(conn->rbuf))
	{
		printf("Not enough space in recive buffer\n");
		return false;
	}

	// remove the requests from the buffer.
	size_t remain = conn->rbuf_size - conn->rbuf_processed;
	if (remain)
	{
		memmove(conn->rbuf, &conn->rbuf[conn->rbuf_processed], remain);
	}
	conn->rbuf_size = remain;
	conn->rbuf_processed = 0;

	int rv = 0;
	size_t cap = sizeof(conn->rbuf) - conn->rbuf_size;
	rv = recv(conn->socket, &conn->rbuf[conn->rbuf_size], cap, 0);
	if (rv == SOCKET_ERROR && WSAGetLastError() == WSAEWOULDBLOCK)
	{
		return false;
	}
	else if (rv == SOCKET_ERROR)
	{
		printf("Error recv(): %ld\n", WSAGetLastError());
		conn->state = STATE_END;
		return false;
	}
	if (rv == 0) 
	{
		printf("Connection was closed\n");
		conn->state = STATE_END;
		return false;
	}

	conn->rbuf_size += rv;

	if (conn->rbuf_size > sizeof(conn->rbuf))
	{
		printf("Recive buffer overflowed\n");
		conn->state = STATE_END;
		return false;
	}

	while (tryOneRequest(conn)) {}
	return (conn->state == STATE_REQ);
}

static void stateReq(Conn* conn) 
{
	while (tryFillBuffer(conn)) {}
}

static void connectionIO(Conn* conn) 
{
	if (conn->state == STATE_REQ) 
	{
		stateReq(conn);
	}
	else if (conn->state == STATE_RES) 
	{
		stateRes(conn);
	}
}

static int oneRequest(SOCKET socket)
{
	char rbuf[4 + k_max_msg + 1];
	int err = readFull(socket, rbuf, 4);
	if (err < 0)
		return err;

	unsigned int len = 0;
	memcpy(&len, rbuf, 4);
	if (len > k_max_msg)
		return -1;

	err = readFull(socket, &rbuf[4], len);
	if (err < 0)
		return err;

	rbuf[4 + len] = '\0';
	printf("client says: %s\n", &rbuf[4]);

	const char reply[] = "world";
	char wbuf[4 + sizeof(reply)];
	len = (int)strlen(reply);
	memcpy(wbuf, &len, 4);
	memcpy(&wbuf[4], reply, len);
	return writeAll(socket, wbuf, 4 + len);
}

int main()
{
	printf("SERVER START\n");
	WSADATA wsaData;

	int iResult;

	iResult = WSAStartup(MAKEWORD(2, 2), &wsaData);
	if (iResult != 0) {
		printf("WSAStartup failed: %d\n", iResult);
		return 1;
	}

	struct addrinfo* result = NULL, * ptr = NULL, hints;

	ZeroMemory(&hints, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;
	hints.ai_flags = AI_PASSIVE;

	iResult = getaddrinfo(NULL, DEFAULT_PORT, &hints, &result);
	if (iResult != 0) {
		printf("getaddrinfo failed: %d\n", iResult);
		WSACleanup();
		return 1;
	}

	SOCKET ListenSocket = INVALID_SOCKET;
	ListenSocket = socket(result->ai_family, result->ai_socktype, result->ai_protocol);

	if (ListenSocket == INVALID_SOCKET) {
		printf("Error at socket(): %ld\n", WSAGetLastError());
		freeaddrinfo(result);
		WSACleanup();
		return 1;
	}

	unsigned long mode = 1;
	iResult = ioctlsocket(ListenSocket, FIONBIO, &mode);
	if (iResult == SOCKET_ERROR) {
		printf("Error at setting nonblocking io: %ld\n", WSAGetLastError());
		freeaddrinfo(result);
		closesocket(ListenSocket);
		WSACleanup();
		return 1;
	}

	iResult = bind(ListenSocket, result->ai_addr, (int)result->ai_addrlen);
	if (iResult == SOCKET_ERROR) {
		printf("bind failed with error: %d\n", WSAGetLastError());
		freeaddrinfo(result);
		closesocket(ListenSocket);
		WSACleanup();
		return 1;
	}

	if (listen(ListenSocket, SOMAXCONN) == SOCKET_ERROR) {
		printf("Listen failed with error: %ld\n", WSAGetLastError());
		closesocket(ListenSocket);
		WSACleanup();
		return 1;
	}

	std::vector<Conn*> conns;
	std::vector<WSAPOLLFD> poll_args;

	while (true)
	{
		poll_args.clear();
		WSAPOLLFD pfd = { ListenSocket, POLLRDNORM, 0 };
		poll_args.push_back(pfd);

		for (Conn* conn : conns)
		{
			if (!conn)
				continue;
			pollfd pfd = {};
			pfd.fd = conn->socket;
			pfd.events = (conn->state == STATE_REQ) ? POLLRDNORM : POLLWRNORM;
			poll_args.push_back(pfd);
		}

		iResult = WSAPoll(poll_args.data(), poll_args.size(), 30000);
		if (iResult == SOCKET_ERROR)
		{
			printf("poll failed: %d\n", WSAGetLastError());
			closesocket(ListenSocket);
			WSACleanup();
			return 1;
		}

		for (int i = 1; i < poll_args.size(); ++i) 
		{
			if (poll_args[i].revents) 
			{
				Conn* conn = conns[poll_args[i].fd];
				connectionIO(conn);
				if (conn->state == STATE_END) 
				{
					conns[conn->socket] = NULL;
					closesocket(conn->socket);
					free(conn);
				}
			}
		}

		if (poll_args[0].revents) 
		{
			acceptNewConn(conns, ListenSocket);
		}
	}

	closesocket(ListenSocket);
	WSACleanup();

	return 0;
}