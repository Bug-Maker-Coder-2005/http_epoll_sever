#include<iostream>
#include<vector>
#include<unistd.h>
#include<fcntl.h>
#include<sstream>
#include<fstream>
#include<thread>
#include<errno.h>
#include<cctype>
#include<dirent.h>
#include<sys/types.h>
#include<queue>
#include<sys/stat.h>
#include<memory>
#include<time.h>
#include<string.h>
#include<functional>
#include<unordered_map>
#include<netinet/in.h>
#include<sys/socket.h>
#include<sys/epoll.h>
#include"json.hpp"
using json = nlohmann::json;

#define THREAD_NUM 8
#define IDLE_TIMEOUT 60000
#define CHECK_TIME 5000
using namespace std;
unordered_map<string,string> g_users;

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

    static inline string trim(const string& str)
    {
        auto start = str.find_first_not_of(" \t\r\n");
        if(start == string::npos)return "";
        auto end = str.find_last_not_of(" \t\r\n");
        return str.substr(start,end - start + 1);
    }

    
    string getMinm(const string& str)
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

bool json_pars(const string& body,string& username,string& password)
{
    try
    {
        json j = json::parse(body);
        username = j["username"];
        password = j["password"];
        return true;
    }
    catch(...)
    {
        return false;
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

        string key = trim(buf.substr(pos,colon - pos));
        string value = trim(buf.substr(colon + 1,line_end - colon - 1));
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
    unordered_map<string,string> file_cache;

    HttpSever():_port(8080){}
    ~HttpSever(){}

    void start()
    {
        last_check = time(nullptr);
        load_files();
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
            int nready = epoll_wait(_epfd,events.data(),events.size(),CHECK_TIME);  //events.data()用来获取底层数组首元素指针
            auto now = time(nullptr);
            if((now - last_check)*1000 > CHECK_TIME)
            {
                check_time();
                last_check = now;
            }

            for(int i=0;i<nready;++i)
            {
                if(events[i].data.fd == _listenfd)
                {
                    OnAccept(_listenfd);
                    continue;
                }
                if(events[i].events & (EPOLLHUP | EPOLLERR))
                {
                    conn_close(events[i].data.fd);
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

    void make_response(int fd,string& buf,const string& mime_type,string& body)
    {
        auto it = _conn.find(fd);
        bool connect = (it != _conn.end() && it->second.connect_flag);
        buf += "HTTP/1.1 200 OK\r\n";
        buf += "Content-Type: " + mime_type +"\r\n";
        buf += "Content-Length: " + to_string(body.size()) + "\r\n";
        if(connect)
            buf += "Connection: Keep-alive\r\n";
        else
            buf += "Connection: Close\r\n";
        buf += "\r\n";
        buf += body;
    }

        void load_files()
    {
        auto load = [](const string& path){
            fstream f(path,ios::in | ios::binary);
            if(!f)
            {
                perror("load error");
                exit(1);
            }
            return string(istreambuf_iterator<char>(f),istreambuf_iterator<char>());
        };
        DIR* dir = opendir("./html");
        if(!dir)
        {
            perror("opendir error");
            exit(1);
        }
        
        struct dirent* file;
        while((file=readdir(dir)) != nullptr)
        {
            if(file->d_name[0] == '.') continue;

            string path = string("./html/") + file->d_name;
            struct stat st;
            if(stat(path.c_str(),&st) < 0)continue;
            if(!S_ISREG(st.st_mode))continue;

            file_cache[path] = load(path);
        }
        closedir(dir);
    }

    void check_time()
    {
        auto now = time(nullptr);
        vector<int> close_vec;
        for(auto &it: _conn)
        {
            if((now - it.second.last_time)*1000 >= IDLE_TIMEOUT)
            {
                close_vec.push_back(it.first);
            }
        }
          for(auto &fd: close_vec)
        {
            conn_close(fd);
        }
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
            
            auto now = time(nullptr);
            _conn[fd].last_time = now;

            _conn[fd].rbuf.append(temp,n);
        }

        if(_conn[fd].rbuf.size() > 8192)
        {
            conn_close(fd);
            return;
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
                if(req.method == "POST" && req.path == "/api/echo")
                {
                    _conn[fd].wbuf += "HTTP/1.1 200 OK\r\n";
                    _conn[fd].wbuf += "Content-Type: application/json\r\n";
                    _conn[fd].wbuf += "Content-Length: " + to_string(req.body.size()) + "\r\n";
                    _conn[fd].wbuf += "\r\n";
                    _conn[fd].wbuf += req.body;
                    continue;
                }
                if(req.method == "POST" && req.path == "/api/register")
                {
                    string username,password;
                    if(!json_pars(req.body,username,password))
                    {
                        _conn[fd].wbuf += "HTTP/1.1 400 Bad Request\r\n";
                        _conn[fd].wbuf += "Content-Type: application/json\r\n";
                        _conn[fd].wbuf += "Content-Length: 26\r\n";
                        _conn[fd].wbuf += "\r\n";
                        _conn[fd].wbuf += R"({"msg":"invalid json"})";
                        continue;
                    }
                    g_users[username] = password;
                    string resp = R"({"msg":"ok"})";
                    _conn[fd].wbuf += "HTTP/1.1 200 OK\r\n";
                    _conn[fd].wbuf += "Content-Type: application/json\r\n";
                    _conn[fd].wbuf += "Content-Length: " + to_string(resp.size()) + "\r\n";
                    _conn[fd].wbuf += "\r\n";
                    _conn[fd].wbuf += resp;
                    continue;
                }
                if(req.method == "POST" && req.path == "/api/login")
                {
                    string username,password;
                    if(!json_pars(req.body,username,password))
                    {
                        _conn[fd].wbuf += "HTTP/1.1 400 Bad Request\r\n";
                        _conn[fd].wbuf += "Content-Type: application/json\r\n";
                        _conn[fd].wbuf += "Content-Length: 26\r\n";
                        _conn[fd].wbuf += "\r\n";
                        _conn[fd].wbuf += R"({"msg":"invalid json"})";
                        continue;
                    }
                    auto it = g_users.find(username);
                    if(it == g_users.end() || it->second != password)
                    {
                        _conn[fd].wbuf += "HTTP/1.1 401 Unauthorized\r\n";
                        _conn[fd].wbuf += "Content-Type: application/json\r\n";
                        _conn[fd].wbuf += "Content-Length: 29\r\n";
                        _conn[fd].wbuf += "\r\n";
                        _conn[fd].wbuf += R"({"msg":"wrong user or pass"})";
                        continue;
                    }
                    json resp;
                    resp["msg"] = "ok";
                    string resp_str = resp.dump();
                    _conn[fd].wbuf += "HTTP/1.1 200 OK\r\n";
                    _conn[fd].wbuf += "Content-Type: application/json\r\n";
                    _conn[fd].wbuf += "Content-Length: " + to_string(resp_str.size()) + "\r\n";
                    _conn[fd].wbuf += "\r\n";
                    _conn[fd].wbuf += resp_str;
                    continue;
                }

                string path = req.path;
                string file_path;
                if(path == "/" || path == "/index.html" || path == "/index")
                {
                    file_path = "./html/index.html";
                }
                else if(path == "/profile.html" || path == "/profile")
                {
                    file_path = "./html/profile.html";
                }
                else if(path == "/data.json")
                {
                    file_path = "./html/data.json";
                }
                else if(path == "/app.js")
                {
                    file_path = "./html/app.js";
                }
                else if(path == "/style.css")
                {
                    file_path = "./html/style.css";
                }
                else if(path == "/readme.txt")
                {
                    file_path = "./html/readme.txt";
                }
                else
                {
                    _conn[fd].wbuf = make_404(fd);
                    break;
                }

                auto it = file_cache.find(file_path);
                if(it == file_cache.end())
                {
                    perror("file_cache find");
                    _conn[fd].wbuf = make_404(fd);
                    continue;
                }
                string &body = it->second;
                string mime_type = getMinm(file_path);
                make_response(fd,_conn[fd].wbuf,mime_type,body);
                break;
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
            if(connfd == -1)
            {
                if(errno == EAGAIN || errno == EWOULDBLOCK)
                    break;
                if(errno == EINTR)
                    continue;
                
                perror("accept error");
                break;
            }
            
            auto now = time(nullptr);
            _conn[connfd].last_time = now;
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
        {
            auto now = time(nullptr);
            _conn[fd].last_time = now;
            conn.wbuf.erase(0,n);
        }
    }

    if(conn.connect_flag == false)
    {
        conn_close(fd);
        return;
    }
    set_ev(fd,EPOLLIN | EPOLLET | EPOLLOUT,0);
} 

    struct Conncontext
    {
        string wbuf;
        string rbuf;
        bool connect_flag = true;
        time_t last_time = time(nullptr);
    };

    int _epfd;
    int _listenfd;
    int _port;
    time_t last_check;
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