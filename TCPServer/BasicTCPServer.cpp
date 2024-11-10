#include <winsock2.h>
#include <ws2tcpip.h>
#include <stdio.h>
#include <stdint.h>
#include <vector>
#include <string>
#include <map>
#include "DataStorage.h"

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

struct Entry 
{
	struct Node node;
	std::string key;
	std::string val;
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

static int32_t acceptNewConn(std::vector<Conn*>& conns, SOCKET socket) 
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

static unsigned long long str_hash(const char* data, unsigned long long len) 
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

static unsigned int doGet(std::vector<std::string>& cmd, char* res, unsigned int* reslen)
{

	Entry entry;
	entry.key.swap(cmd[1]);
	entry.node.hash_code = str_hash(entry.key.data(), entry.key.size());

	Node* n = hashMapLookup(&g_data.db, &entry.node, &entryEq);
	if (!n) 
	{
		return RES_NX;
	}

	struct Entry* e = container_of(n, struct Entry, node);
	const std::string& val = e->val;
	if (val.size() > k_max_msg)
	{
		printf("Node value too big\n");
		return RES_ERR;
	}

	memcpy(res, val.data(), val.size());
	*reslen = (unsigned int)val.size();
	return RES_OK;
}

static unsigned int doSet(std::vector<std::string>& cmd, char* res, unsigned int* reslen)
{
	(void)res;
	(void)reslen;
	
	Entry entry;
	entry.key.swap(cmd[1]);
	entry.node.hash_code = str_hash(entry.key.data(), entry.key.size());

	Node* node = hashMapLookup(&g_data.db, &entry.node, &entryEq);
	if (node) 
	{
		struct Entry* e = container_of(node, struct Entry, node);
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
	return RES_OK;
}

static unsigned int doDel(std::vector<std::string>& cmd, char* res, unsigned int* reslen)
{
	(void)res;
	(void)reslen;

	Entry entry;
	entry.key.swap(cmd[1]);
	entry.node.hash_code = str_hash(entry.key.data(), entry.key.size());

	Node* node = hashMapPop(&g_data.db, &entry.node, &entryEq);
	if (node) 
	{
		delete container_of(node, Entry, node);
	}
	return RES_OK;
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

static int doRequest(const char* req, unsigned int reqlen, unsigned int* rescode, char* res, unsigned int* reslen)
{
	std::vector<std::string> cmd;
	if (0 != parseReq(req, reqlen, cmd)) 
	{
		printf("bad request\n");
		return -1;
	}
	if (cmd.size() == 2 && cmdIs(cmd[0], "get")) 
	{
		*rescode = doGet(cmd, res, reslen);
	}
	else if (cmd.size() == 3 && cmdIs(cmd[0], "set")) 
	{
		*rescode = doSet(cmd, res, reslen);
	}
	else if (cmd.size() == 2 && cmdIs(cmd[0], "del")) 
	{
		*rescode = doDel(cmd, res, reslen);
	}
	else 
	{
		*rescode = RES_ERR;
		const char* msg = "Unknown cmd";
		strcpy_s(res, _TRUNCATE, msg);
		*reslen = strlen(msg);
		return 0;
	}
	return 0;
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

	unsigned int rescode = 0;
	unsigned int wlen = 0;
	int err = doRequest(&conn->rbuf[4], len, &rescode, &conn->wbuf[4 + 4], &wlen);
	if (err) 
	{
		conn->state = STATE_END;
		return false;
	}
	wlen += 4;
	memcpy(&conn->wbuf[0], &wlen, 4);
	memcpy(&conn->wbuf[4], &rescode, 4);
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
	if (conn->state == STATE_REQ) {
		stateReq(conn);
	}
	else if (conn->state == STATE_RES) {
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
			//pfd.events = pfd.events | POLLERR;
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

		for (int i = 1; i < poll_args.size(); ++i) {
			if (poll_args[i].revents) 
			{
				Conn* conn = conns[poll_args[i].fd];
				connectionIO(conn);
				if (conn->state == STATE_END) {
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