#include <winsock2.h>
#include <ws2tcpip.h>
#include <stdio.h>
#include <vector>
#include <string>

#pragma comment(lib, "Ws2_32.lib")

#define DEFAULT_PORT "27015"
#define DEFAULT_BUFLEN 512

const int k_max_msg = 4096;

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

static int sendReq(SOCKET socket, const std::vector<std::string>& cmd) 
{
    unsigned int len = 4;
    for (const std::string& s : cmd) 
    {
        len += 4 + s.size();
    }
    if (len > k_max_msg) 
    {
        return -1;
    }

    char wbuf[4 + k_max_msg];
    memcpy(&wbuf[0], &len, 4);
    unsigned int n = cmd.size();
    memcpy(&wbuf[4], &n, 4);
    size_t cur = 8;
    for (const std::string& s : cmd) 
    {
        unsigned int p = (unsigned int)s.size();
        memcpy(&wbuf[cur], &p, 4);
        memcpy(&wbuf[cur + 4], s.data(), s.size());
        cur += 4 + s.size();
    }
    return writeAll(socket, wbuf, 4 + len);
}

static int readRes(SOCKET socket) 
{
    char rbuf[4 + k_max_msg + 1];
    errno = 0;
    int err = readFull(socket, rbuf, 4);
    if (err) 
    {
        printf("recv() error\n");
        return err;
    }

    unsigned int len = 0;
    memcpy(&len, rbuf, 4);
    if (len > k_max_msg) 
    {
        printf("message too long\n");
        return -1;
    }

    err = readFull(socket, &rbuf[4], len);
    if (err) 
    {
        printf("recv() error\n");
        return err;
    }

    unsigned int rescode = 0;
    if (len < 4) 
    {
        printf("bad response\n");
        return -1;
    }
    memcpy(&rescode, &rbuf[4], 4);
    printf("server says: [%u] %.*s\n", rescode, len - 4, &rbuf[8]);
    return 0;
}


static int query(SOCKET socket, const char* text) 
{
    unsigned int len = (unsigned int)strlen(text);
    if (len > k_max_msg)
        return -1;

    char wbuf[4 + k_max_msg];
    memcpy(wbuf, &len, 4);
    memcpy(&wbuf[4], text, len);
    int err = writeAll(socket, wbuf, 4 + len);
    if (err<0)
        return err;

    char rbuf[4 + k_max_msg + 1];
    err = readFull(socket, rbuf, 4);
    if (err < 0)
        return err;

    memcpy(&len, rbuf, 4); 
    if (len > k_max_msg)
        return -1;

    err = readFull(socket, &rbuf[4], len);
    if (err<0)
        return err;

    rbuf[4 + len] = '\0';
    printf("server says: %s\n", &rbuf[4]);
    return 0;
}

int sendAndRecive(SOCKET ConnectSocket)
{

    int recvbuflen = DEFAULT_BUFLEN;

    const char* sendbuf = "test message";
    char recvbuf[DEFAULT_BUFLEN];

    int iResult;

    iResult = send(ConnectSocket, sendbuf, (int)strlen(sendbuf), 0);
    if (iResult == SOCKET_ERROR) {
        printf("send failed: %d\n", WSAGetLastError());
        closesocket(ConnectSocket);
        WSACleanup();
        return 1;
    }

    printf("Bytes Sent: %ld\n", iResult);

    iResult = shutdown(ConnectSocket, SD_SEND);
    if (iResult == SOCKET_ERROR) {
        printf("shutdown failed: %d\n", WSAGetLastError());
        closesocket(ConnectSocket);
        WSACleanup();
        return 1;
    }

    do
    {
        iResult = recv(ConnectSocket, recvbuf, recvbuflen, 0);
        if (iResult > 0)
        {
            printf("Bytes received: %d\n", iResult);
            printf("Revived message: %.*s\n", (int)sizeof(recvbuf), recvbuf);
        }
        else if (iResult == 0)
            printf("Connection closed\n");
        else
            printf("recv failed: %d\n", WSAGetLastError());
    } while (iResult > 0);
}

int main(int argc, char** argv)
{
    int iResult;
    WSADATA wsaData;

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

    iResult = getaddrinfo("127.0.0.1", DEFAULT_PORT, &hints, &result);
    if (iResult != 0) {
        printf("getaddrinfo failed: %d\n", iResult);
        WSACleanup();
        return 1;
    }

    SOCKET ConnectSocket = INVALID_SOCKET;
    ptr = result;
    ConnectSocket = socket(ptr->ai_family, ptr->ai_socktype, ptr->ai_protocol);
    if (ConnectSocket == INVALID_SOCKET) {
        printf("Error at socket(): %ld\n", WSAGetLastError());
        freeaddrinfo(result);
        WSACleanup();
        return 1;
    }

    iResult = connect(ConnectSocket, ptr->ai_addr, (int)ptr->ai_addrlen);
    if (iResult == SOCKET_ERROR) {
        closesocket(ConnectSocket);
        ConnectSocket = INVALID_SOCKET;
    }

    freeaddrinfo(result);

    if (ConnectSocket == INVALID_SOCKET) {
        printf("Unable to connect to server!\n");
        WSACleanup();
        return 1;
    }

    std::vector<std::string> cmd;
    cmd.push_back("set");
    cmd.push_back("k");
    cmd.push_back("v");

    int err = sendReq(ConnectSocket, cmd);
    if (err) 
    {
        closesocket(ConnectSocket);
        WSACleanup();
        return -1;
    }
    err = readRes(ConnectSocket);
    if (err) 
    {
        closesocket(ConnectSocket);
        WSACleanup();
        return -1;
    }
    cmd.clear();
    cmd.push_back("get");
    cmd.push_back("k");
    err = sendReq(ConnectSocket, cmd);
    if (err)
    {
        closesocket(ConnectSocket);
        WSACleanup();
        return -1;
    }
    err = readRes(ConnectSocket);
    if (err)
    {
        closesocket(ConnectSocket);
        WSACleanup();
        return -1;
    }
    cmd.clear();
    cmd.push_back("del");
    cmd.push_back("k");
    err = sendReq(ConnectSocket, cmd);
    if (err)
    {
        closesocket(ConnectSocket);
        WSACleanup();
        return -1;
    }
    err = readRes(ConnectSocket);
    if (err)
    {
        closesocket(ConnectSocket);
        WSACleanup();
        return -1;
    }
    cmd.clear();
    cmd.push_back("get");
    cmd.push_back("k");
    err = sendReq(ConnectSocket, cmd);
    if (err)
    {
        closesocket(ConnectSocket);
        WSACleanup();
        return -1;
    }
    err = readRes(ConnectSocket);
    if (err)
    {
        closesocket(ConnectSocket);
        WSACleanup();
        return -1;
    }

    closesocket(ConnectSocket);
    WSACleanup();

    return 0;
}