#include<iostream>
#include<vector>
#include<unistd.h>
#include<fcntl.h>
#include<sstream>
#include<fstream>
#include<thread>
#include<queue>
#include<memory>
#include<functional>
#include<unordered_map>
#include<netinet/in.h>
#include<sys/socket.h>
#include<sys/epoll.h>

#define THREAD_NUM 8
using namespace std;

namespace
{
    struct HttpRequest
    {
        string method;  //请求方法
        string path;    //请求文件路径
        string version; //请求协议版本
        unordered_map<string,string> header;    //请求头
        string body;    //请求体
        size_t content_length = 0;  //body长度
    };

    void read_file(string &path,string& buf)
    {
        int fd = open(path.c_str(),O_RDONLY);
        if(fd < 0)
        {
            buf.clear();
            return;
        }

        off_t size = lseek(fd,0,SEEK_END);
        if(size < 0)
        {
            close(fd);
            buf.clear();
            return;
        }
        
        if(lseek(fd,0,SEEK_SET) < 0)
        {
            close(fd);
            buf.clear();
            return;
        }

        buf.resize(size);
        size_t total = 0;
        while(total < (size_t)size)
        {
            int n = read(fd,&buf[total],size - total);
            if(n == -1)
            {
                if(errno == EINTR)
                    continue;

                buf.clear();
                close(fd);
                return;
            }
            else if(n == 0)
            {
                break;
            }
            total += n;
        }
        buf.resize(total);
        close(fd);
    }
    
    string getmine(string& str)
    {
        size_t dot = str.rfind('.');
        if(dot == string::npos)
            return "application/octet-stream";
        
        string file = str.substr(dot);
        if (file == ".html" || file == ".htm") return "text/html";
        if (file == ".css") return "text/css";
        if (file == ".js") return "application/javascript";
        if (file == ".png")  return "image/png";
        if (file == ".jpg" || file == ".jpeg") return "image/jpeg";
        if (file == ".gif")  return "image/gif";
        if (file == ".ico")  return "image/x-icon";
        if (file == ".json") return "application/json";
        if (file == ".txt")  return "text/plain";
        return "application/octet-stream";
    }
}

//http请求解析
bool http_pars(HttpRequest &req,string &buf,size_t &consumed)
{
    //找前部分结束
    size_t header_end = buf.find("\r\n\r\n",0);
    if(header_end == string::npos)
        return false;
    
    //解析请求行
    size_t line_end = buf.find("\r\n",0);
    string line = buf.substr(0,line_end);
    istringstream iss(line);
    iss >> req.method >> req.path >> req.version;
    if(req.path.empty() || req.version.empty())
        return false;

    //解析请求头
    size_t pos = line_end + 2;
    while(pos < header_end)
    {
        size_t line_end = buf.find("\r\n",pos);
        size_t colon = buf.find(':',pos);
        if(colon == string::npos)
        {
            pos = line_end + 2;
            continue;
        }

        string key = buf.substr(pos,colon - pos);
        string value = buf.substr(colon + 1,line_end - colon - 1);
        req.header[key] = value;
        pos = line_end + 2;
        
        
    }
    //检查body长度
    size_t body_start = header_end + 4;
    auto it = req.header.find("Content-Length");
    if(it != req.header.end())
    {
        req.content_length = stoul(it->second);
    }

    //body是否收全
    if(buf.size() < body_start + req.content_length)
        return false;

    req.body = buf.substr(body_start,req.content_length);
    consumed = body_start + req.content_length;

    return true;
}


class HttpSever
{
public:
    HttpSever():_port(8080){}
    ~HttpSever(){}

    void start()
    {
        _listenfd = socket(AF_INET,SOCK_STREAM,0);
        int flags = fcntl(_listenfd,F_GETFL,0);
        
        struct sockaddr_in servaddr;
        servaddr.sin_family = AF_INET;
        servaddr.sin_port = htons(_port);
        servaddr.sin_addr.s_addr = htonl(INADDR_ANY);

        socklen_t servlen = sizeof(servaddr);

        int opt = 1;
        setsockopt(_listenfd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));  //设置端口复用，检测死连接，检测对端是否断开，崩溃
        setsockopt(_listenfd,SOL_SOCKET,SO_KEEPALIVE,&opt,sizeof(opt));
        if(-1 == setsockopt(_listenfd,SOL_SOCKET,SO_REUSEPORT,&opt,sizeof(opt)))
        {
            perror("reuserport faliure");
            exit(1);
        }
        if(-1 == fcntl(_listenfd,F_SETFL,flags | O_NONBLOCK))
        {
            perror("fcntl error");
            exit(1);
        }
        if(bind(_listenfd,(struct sockaddr*)&servaddr,servlen) == -1)
        {
            perror("bind failure");
            exit(1);
        }

        listen(_listenfd,128);
       

        _epfd = epoll_create1(EPOLL_CLOEXEC);
        if(_epfd == -1)
        {
            perror("epoll_create failure");
            exit(1);
        }

        set_ev(_listenfd,EPOLLIN | EPOLLET,1);

        vector<struct epoll_event>events(1024);
        while(1)
        {
            int nready = epoll_wait(_epfd,events.data(),events.size(),-1);  //events.data()用来获取底层数组首元素指针
            for(int i=0;i<nready;++i)
            {
                if(events[i].data.fd == _listenfd)
                {
                    OnAccept(_listenfd);
                    continue;
                }
                if(events[i].events & EPOLLIN)
                {
                    OnRecv(events[i].data.fd);
                }
                if(events[i].events & EPOLLOUT)
                {
                    OnSend(events[i].data.fd);
                }
            }
        }

    }

private:
    std::string make_404(int fd) {
    std::string body = "<html>"
                       "<head><title>404 Not Found</title></head>"
                       "<body><h1>404 Not Found</h1>"
                       "<p>The requested URL was not found.</p>"
                       "</body></html>";
    
    auto it = _conn.find(fd);
    bool connect = (it != _conn.end() && it->second.connect_flag);
    std::string resp;
    resp += "HTTP/1.1 404 Not Found\r\n";
    resp += "Content-Type: text/html; charset=utf-8\r\n";
    resp += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    if(connect)
        resp += "Connection: keep-alive\r\n";
    else
        resp += "Connection: Close\r\n";
    resp += "\r\n";
    resp += body;
    return resp;
}

    void make_response(int fd,string& buf,string& file,string& body)
    {
        auto it = _conn.find(fd);
        bool connect = (it != _conn.end() && it->second.connect_flag);
        buf += "HTTP/1.1 200 OK\r\n";
        buf += "Content-Type:" + file +"\r\n";
        buf += "Content-Length:" + to_string(body.size()) + "\r\n";
        if(connect)
            buf += "Connection: Keep-alive\r\n";
        else
            buf += "Connection: Close\r\n";
        buf += "\r\n";
        buf += body;
    }

    void conn_close(int fd)
    {
        auto it = _conn.find(fd);
        if(it == _conn.end()) return;
        epoll_ctl(_epfd,EPOLL_CTL_DEL,fd,nullptr);
        close(fd);
        _conn.erase(fd);
    }
    void OnRecv(int fd)
    {
        char temp[4096];
        while(1)
        {
            int n = recv(fd,temp,4096,0);
            if(n == 0)
            {
                conn_close(fd);
                return;
            }
            if(n == -1)
            {
                if(errno == EAGAIN || errno == EWOULDBLOCK)//正常读完
                    break;
                if(errno == EINTR)//被信号打断，重试
                    continue;
                conn_close(fd);
                return;
            }

            _conn[fd].rbuf.append(temp,n);
        }

        HttpRequest req;
        size_t consumed = 0;
        while(true)
        {
            if(http_pars(req,_conn[fd].rbuf,consumed))
            {
                if(req.header["Connection"] == "Close")
                {
                    _conn[fd].connect_flag = false;
                }
                _conn[fd].rbuf.erase(0,consumed);
                _conn[fd].wbuf.clear();

                string path = req.path;
                string body,file_path;
                if(path == "/" || path == "/index.html" || path == "/index")
                {
                    file_path = "./html/index.html";
                    read_file(file_path,body);
                }
                else if(path == "/profile.html" || path == "/profile")
                {
                    file_path = "./html/profile.html";
                    read_file(file_path,body);
                }
                else
                {
                    _conn[fd].wbuf = make_404(fd);
                    break;
                }

                string file = getmine(file_path);
                make_response(fd,_conn[fd].wbuf,file,body);
            }
            else
            {
                break;
            }
        }
        if(!_conn[fd].wbuf.empty())
        {
            set_ev(fd,EPOLLOUT | EPOLLET,0);
        }
    }

    void OnSend(int fd)
    {
        do_response(fd);
    }

    void OnAccept(int fd)
    {
        struct sockaddr_in clieaddr;
        socklen_t clielen = sizeof(clieaddr);

        while(1)
        {
            int connfd = accept(fd,(struct sockaddr*)&clieaddr,&clielen);
            if(connfd == -1 && errno == EAGAIN)
                break;

            int flags = fcntl(connfd,F_GETFL,0);
            fcntl(connfd,F_SETFL,flags | O_NONBLOCK);

            set_ev(connfd,EPOLLIN | EPOLLET,1);
        }
    }
    //设置监听事件并加入监听红黑树中
    void set_ev(int fd,int events,int flag)
    {
        struct epoll_event ev;
        ev.data.fd = fd;
        ev.events = events;
        if(flag)
        {
            if(epoll_ctl(_epfd,EPOLL_CTL_ADD,fd,&ev) == -1)
            {
                perror("epoll_ctl failure");
                exit(1);
            }
        }
        else
        {
            epoll_ctl(_epfd,EPOLL_CTL_MOD,fd,&ev);
        }
    }

void do_response(int fd)
{
    auto &conn = _conn[fd];
    while(!conn.wbuf.empty())
    {
        int n = send(fd,conn.wbuf.data(),conn.wbuf.size(),MSG_NOSIGNAL);
        if(n == -1)
        {
            if(errno == EAGAIN || errno == EWOULDBLOCK)
            {
                return;
            }
            else if(errno == EINTR)
            {
                continue;
            }
            else
            {
                conn_close(fd);
                return;
            }
        }
        if(n > 0)
        conn.wbuf.erase(0,n);
    }
    if(conn.connect_flag == false)
    {
        conn_close(fd);
        return;
    }
    set_ev(fd,EPOLLIN | EPOLLET,0);
} 

    struct Conncontext
    {
        string wbuf;
        string rbuf;
        bool connect_flag = true;
    };

    int _epfd;
    int _listenfd;
    int _port;
    unordered_map<int,Conncontext> _conn;
};

int main(void)
{
    vector<thread> vec;
    for(int i=0;i<THREAD_NUM;++i)
    {
        vec.emplace_back([&](){
            HttpSever sever;
            sever.start();
        });
    }
    for(auto &t : vec)
    {
        t.join();
    }
    

    return 0;
}