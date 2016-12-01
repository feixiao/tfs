/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: sync_by_blk.cpp 2463 2013-08-06 02:23:41Z chuyu $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */

#include "sync_file_base.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "message/block_info_message.h"
#include "message/server_status_message.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::client;

typedef vector<uint32_t> BLOCK_ID_VEC;
typedef vector<uint32_t>::iterator BLOCK_ID_VEC_ITER;
typedef vector<std::string> FILE_NAME_VEC;
typedef vector<std::string>::iterator FILE_NAME_VEC_ITER;

struct BlockFileInfo
{
  BlockFileInfo(const uint32_t block_id, const FileInfo& file_info)
    : block_id_(block_id)
  {
    memcpy(&file_info_, &file_info, sizeof(file_info));
  }
  ~BlockFileInfo()
  {
  }
  uint32_t block_id_;
  FileInfo file_info_;
};

typedef vector<BlockFileInfo> BLOCK_FILE_INFO_VEC;
typedef vector<BlockFileInfo>::iterator BLOCK_FILE_INFO_VEC_ITER;

struct SyncStat
{
  int64_t total_count_;
  int64_t actual_count_;
  int64_t success_count_;
  int64_t fail_count_;
  int64_t unsync_count_;
  int64_t del_count_;
};
struct LogFile
{
  FILE** fp_;
  const char* file_;
};

tbutil::Mutex g_mutex_;
SyncStat g_sync_stat_;
static int32_t thread_count = 1;
static const int32_t MAX_READ_LEN = 256;
FILE *g_blk_done= NULL;
FILE *g_file_succ = NULL, *g_file_fail = NULL, *g_file_unsync = NULL, *g_file_del = NULL;

//int get_server_status();
//int get_diff_block();
int get_file_list(const string& ns_addr, const uint32_t block_id, BLOCK_FILE_INFO_VEC& v_block_file_info);
int get_file_list(const string& ns_addr, const uint32_t block_id, FILE_NAME_VEC& v_file_name);
int sync_file(const string& src_ns_addr, const string& dest_ns_addr, const uint32_t block_id, FileInfo& file_info, const bool force, const int32_t modify_time);

class WorkThread : public tbutil::Thread
{
  public:
    WorkThread(const string& src_ns_addr, const string& dest_ns_addr, const bool need_remove_file, const bool force, const int32_t modify_time):
      src_ns_addr_(src_ns_addr), dest_ns_addr_(dest_ns_addr), need_remove_file_(need_remove_file), force_(force), modify_time_(modify_time), destroy_(false)
    {
    }
    virtual ~WorkThread()
    {
    }

    void wait_for_shut_down()
    {
      join();
    }

    void destroy()
    {
      destroy_ = true;
    }

    virtual void run()
    {
      if (!destroy_)
      {
        BLOCK_ID_VEC_ITER iter = v_block_id_.begin();
        TBSYS_LOG(INFO, "thread block size: %zd", v_block_id_.size());
        for (; iter != v_block_id_.end(); iter++)
        {
          if (!destroy_)
          {
            uint32_t block_id = (*iter);
            bool block_done = false;
            TBSYS_LOG(DEBUG, "sync block started. blockid: %u", block_id);

            BLOCK_FILE_INFO_VEC v_block_file_info;
            get_file_list(src_ns_addr_, block_id, v_block_file_info);
            TBSYS_LOG(DEBUG, "file size: %zd", v_block_file_info.size());
            BLOCK_FILE_INFO_VEC_ITER file_info_iter = v_block_file_info.begin();
            for (; file_info_iter != v_block_file_info.end(); file_info_iter++)
            {
              if (destroy_)
              {
                break;
              }
              sync_file(src_ns_addr_, dest_ns_addr_, block_id, (*file_info_iter).file_info_, force_, modify_time_);
            }
            block_done = (file_info_iter == v_block_file_info.end());

            if (need_remove_file_)
            {
              block_done = false;
              if (!destroy_)
              {
                FILE_NAME_VEC src_v_file_name, dest_v_file_name, diff_v_file_name;

                get_file_list(src_ns_addr_, block_id, src_v_file_name);
                TBSYS_LOG(INFO, "source file list size: %zd", src_v_file_name.size());

                get_file_list(dest_ns_addr_, block_id, dest_v_file_name);
                TBSYS_LOG(INFO, "dest file list size: %zd", dest_v_file_name.size());

                get_diff_file_list(src_v_file_name, dest_v_file_name, diff_v_file_name);
                TBSYS_LOG(INFO, "diff file list size: %zd", diff_v_file_name.size());

                unlink_file_list(diff_v_file_name);
                TBSYS_LOG(INFO, "unlink file list size: %zd", diff_v_file_name.size());

                block_done = true;
              }
            }

            if (block_done)
            {
              fprintf(g_blk_done, "%u\n", block_id);
              TBSYS_LOG(INFO, "sync block finished. blockid: %u", block_id);
            }
          }
        }
      }
    }

    void push_back(uint32_t block_id)
    {
      v_block_id_.push_back(block_id);
    }

    void get_diff_file_list(FILE_NAME_VEC& src_v_file_name, FILE_NAME_VEC& dest_v_file_name, FILE_NAME_VEC& out_v_file_name)
    {
      FILE_NAME_VEC_ITER dest_iter = dest_v_file_name.begin();
      for(; dest_iter != dest_v_file_name.end(); dest_iter++)
      {
        vector<std::string>::iterator src_iter = src_v_file_name.begin();
        for(; src_iter != src_v_file_name.end(); src_iter++)
        {
          if (*src_iter == *dest_iter)
          {
            break;
          }
        }
        if (src_iter == src_v_file_name.end())
        {
          TBSYS_LOG(DEBUG, "dest file need to be deleted. file_name: %s", (*dest_iter).c_str());
          fprintf(g_file_del, "%s\n", (*dest_iter).c_str());
          out_v_file_name.push_back(*dest_iter);
        }
      }
    }

    int64_t unlink_file_list(vector<std::string>& out_v_file_name)
    {
      int64_t size = 0;
      vector<std::string>::iterator iter = out_v_file_name.begin();
      for(; iter != out_v_file_name.end(); iter++)
      {
        TfsClientImpl::Instance()->unlink(size, (*iter).c_str(), NULL, dest_ns_addr_.c_str());
        TBSYS_LOG(DEBUG, "unlink file(%s) succeed.", (*iter).c_str());
        {
          tbutil::Mutex::Lock lock(g_mutex_);
          g_sync_stat_.del_count_++;
        }
        fprintf(g_file_del, "%s\n", (*iter).c_str());
      }
      return size;
    }

  private:
    WorkThread(const WorkThread&);
    string src_ns_addr_;
    string dest_ns_addr_;
    bool need_remove_file_;
    bool force_;
    BLOCK_ID_VEC v_block_id_;
    int32_t modify_time_;
    bool destroy_;
};
typedef tbutil::Handle<WorkThread> WorkThreadPtr;

static WorkThreadPtr* gworks = NULL;

struct LogFile g_log_fp[] =
{
  {&g_file_succ, "sync_succ_file"},
  {&g_file_fail, "sync_fail_file"},
  {&g_file_unsync, "sync_unsync_file"},
  {&g_file_del, "sync_del_file"},
  {NULL, NULL}
};
static string sync_file_log("sync_by_blk.log");

int rename_file(const char* file_path)
{
  int ret = TFS_SUCCESS;
  if (access(file_path, F_OK) == 0)
  {
    char old_file_path[256];
    snprintf(old_file_path, 256, "%s.%s", file_path, Func::time_to_str(time(NULL), 1).c_str());
    ret = rename(file_path, old_file_path);
  }
  return ret;
}

int init_log_file(const char* dir_path)
{
  for (int i = 0; g_log_fp[i].file_; i++)
  {
    char file_path[256];
    snprintf(file_path, 256, "%s/%s", dir_path, g_log_fp[i].file_);
    rename_file(file_path);
    *g_log_fp[i].fp_ = fopen(file_path, "wb");
    if (!*g_log_fp[i].fp_)
    {
      printf("open file fail %s : %s\n:", g_log_fp[i].file_, strerror(errno));
      return TFS_ERROR;
    }
  }
  char log_file_path[256];
  snprintf(log_file_path, 256, "%s/%s", dir_path, sync_file_log.c_str());
  rename_file(log_file_path);
  TBSYS_LOGGER.setFileName(log_file_path, true);
  TBSYS_LOGGER.setMaxFileSize(1024 * 1024 * 1024);

  return TFS_SUCCESS;
}

static void usage(const char* name)
{
  fprintf(stderr, "Usage: %s -s src_addr -d dest_addr -f blk_list [-m modify_time] [-t thread_count] [-p log_path] [-e] [-u] [-l level] [-h]\n", name);
  fprintf(stderr, "       -s source ns ip port\n");
  fprintf(stderr, "       -d dest ns ip port\n");
  fprintf(stderr, "       -f block list file, assign a file to sync\n");
  fprintf(stderr, "       -m modify time, the file modified after it will be ignored. ex: 20121220, default is tomorrow, optional\n");
  fprintf(stderr, "       -e force flag, need strong consistency, optional\n");
  fprintf(stderr, "       -u flag, need delete redundent file in dest cluster, optional\n");
  fprintf(stderr, "       -p the dir path of log file and result logs, both absolute and relative path are ok, if path not exists, it will be created. default is under logs/, optional\n");
  fprintf(stderr, "       -l log file level, set log file level to (debug|info|warn|error), default is info, optional\n");
  fprintf(stderr, "       -h help\n");
  fprintf(stderr, "ex: ./sync_by_blk -s 168.192.1.1:3000 -d 168.192.1.2:3000 -f blk_list -m 20121220\n");
  fprintf(stderr, "    ./sync_by_blk -s 168.192.1.1:3000 -d 168.192.1.2:3000 -f blk_list -m 20121220 -p t1m -t 5\n");
  exit(TFS_ERROR);
}

static void interrupt_callback(int signal)
{
  TBSYS_LOG(INFO, "application signal[%d]", signal);
  switch( signal )
  {
    case SIGTERM:
    case SIGINT:
    default:
      if (gworks != NULL)
      {
        for (int32_t i = 0; g_log_fp[i].file_; i++)
        {
          if (!*g_log_fp[i].fp_)
          {
            fflush(*g_log_fp[i].fp_);
          }
        }
        for (int32_t i = 0; i < thread_count; ++i)
        {
          if (gworks != 0)
          {
            gworks[i]->destroy();
          }
        }
      }
      break;
  }
}

string get_day(const int32_t days, const bool is_after)
{
  time_t now = time((time_t*)NULL);
  time_t end_time;
  if (is_after)
  {
    end_time = now + 86400 * days;
  }
  else
  {
    end_time = now - 86400 * days;
  }
  char tmp[64];
  struct tm *ttime;
  ttime = localtime(&end_time);
  strftime(tmp,64,"%Y%m%d",ttime);
  return string(tmp);
}

int main(int argc, char* argv[])
{
  int32_t i;
  string src_ns_addr, dest_ns_addr;
  string block_list_file;
  bool force = false;
  bool need_remove_file = false;
  string modify_time = get_day(1, true);
  string log_path("logs/");
  string level("info");

  // analyze arguments
  while ((i = getopt(argc, argv, "s:d:f:eum:t:p:l:h")) != EOF)
  {
    switch (i)
    {
      case 's':
        src_ns_addr = optarg;
        break;
      case 'd':
        dest_ns_addr = optarg;
        break;
      case 'f':
        block_list_file = optarg;
        break;
      case 'e':
        force = true;
        break;
      case 'u':
        need_remove_file = true;
        break;
      case 'm':
        modify_time = optarg;
        break;
      case 't':
        thread_count = atoi(optarg);
        break;
      case 'p':
        log_path = optarg;
        break;
      case 'l':
        level = optarg;
        break;
      case 'h':
      default:
        usage(argv[0]);
    }
  }

  if (src_ns_addr.empty()
      || dest_ns_addr.empty()
      || block_list_file.empty()
      || modify_time.empty())
  {
    usage(argv[0]);
  }

  if ((level != "info") && (level != "debug") && (level != "error") && (level != "warn"))
  {
    fprintf(stderr, "level(info | debug | error | warn) set error\n");
    return TFS_ERROR;
  }

  if ((access(log_path.c_str(), F_OK) == -1) && (!DirectoryOp::create_full_path(log_path.c_str())))
  {
    TBSYS_LOG(ERROR, "create log file path failed. log_path: %s", log_path.c_str());
    return TFS_ERROR;
  }

  modify_time += "000000";
  init_log_file(log_path.c_str());
  TBSYS_LOGGER.setLogLevel(level.c_str());

  TfsClientImpl::Instance()->initialize(NULL, DEFAULT_BLOCK_CACHE_TIME, 1000, false);

  BLOCK_ID_VEC v_block_id;
  if (! block_list_file.empty())
  {
    int32_t done_blk_count = 0;
    char file_path[256];
    snprintf(file_path, 256, "%s/%s", log_path.c_str(), "sync_done_blk");
    FILE *fp = NULL;
    if((fp = fopen(block_list_file.c_str(), "rb")) == NULL)
    {
      TBSYS_LOG(ERROR, "open fail output file: %s, error: %s\n", block_list_file.c_str(), strerror(errno));
      return TFS_ERROR;
    }
    else if ((g_blk_done = fopen(file_path, "a+")) == NULL)
    {
      TBSYS_LOG(ERROR, "open fail output file: sync_blk_done, error: %s\n", strerror(errno));
      return TFS_ERROR;
    }
    else
    {
      // load done blocks
      uint32_t block_id = 0;
      while (fscanf(g_blk_done, "%u", &block_id) != EOF)
      {
        tbutil::Mutex::Lock lock(g_mutex_);
        done_blk_count++;
      }
      while (fscanf(fp, "%u", &block_id) != EOF)
      {
        if (done_blk_count-- > 0)
        {
          continue;
        }
        v_block_id.push_back(block_id);
      }
      fclose(fp);
    }
  }
  TBSYS_LOG(INFO, "blockid list size: %zd", v_block_id.size());


  memset(&g_sync_stat_, 0, sizeof(g_sync_stat_));

  if (v_block_id.size() > 0)
  {
    gworks = new WorkThreadPtr[thread_count];
    int32_t i = 0;
    int32_t time = tbsys::CTimeUtil::strToTime(const_cast<char*>(modify_time.c_str()));
    for (; i < thread_count; ++i)
    {
      gworks[i] = new WorkThread(src_ns_addr, dest_ns_addr, need_remove_file, force, time);
    }
    int32_t index = 0;
    int64_t count = 0;
    BLOCK_ID_VEC_ITER iter = v_block_id.begin();
    for (; iter != v_block_id.end(); iter++)
    {
      index = count % thread_count;
      gworks[index]->push_back(*iter);
      ++count;
    }
    for (i = 0; i < thread_count; ++i)
    {
      gworks[i]->start();
    }

    signal(SIGTERM, interrupt_callback);
    signal(SIGINT, interrupt_callback);

    for (i = 0; i < thread_count; ++i)
    {
      gworks[i]->wait_for_shut_down();
    }

    tbsys::gDeleteA(gworks);
  }

  for (i = 0; g_log_fp[i].fp_; i++)
  {
    if (*g_log_fp[i].fp_ != NULL)
    {
      fclose(*g_log_fp[i].fp_);
      *g_log_fp[i].fp_ = NULL;
    }
  }
  g_blk_done = NULL;

  fprintf(stdout, "total_count: %"PRI64_PREFIX"d, actual_count: %"PRI64_PREFIX"d, "
      "succ_count: %"PRI64_PREFIX"d, fail_count: %"PRI64_PREFIX"d, unsync_count: %"PRI64_PREFIX"d, del_count: %"PRI64_PREFIX"d\n",
      g_sync_stat_.total_count_, g_sync_stat_.actual_count_,
      g_sync_stat_.success_count_, g_sync_stat_.fail_count_, g_sync_stat_.unsync_count_, g_sync_stat_.del_count_);
  fprintf(stdout, "log and result path: %s\n", log_path.c_str());

  return TFS_SUCCESS;
}
int get_file_list(const string& ns_addr, const uint32_t block_id, FILE_NAME_VEC& v_file_name)
{
  BLOCK_FILE_INFO_VEC v_block_file_info;
  int ret = get_file_list(ns_addr, block_id, v_block_file_info);
  if (ret == TFS_SUCCESS)
  {
    BLOCK_FILE_INFO_VEC_ITER iter = v_block_file_info.begin();
    for (; iter != v_block_file_info.end(); iter++)
    {
      FSName fsname(iter->block_id_, iter->file_info_.id_, TfsClientImpl::Instance()->get_cluster_id(ns_addr.c_str()));
      v_file_name.push_back(string(fsname.get_name()));
    }
  }
  return ret;
}
int get_file_list(const string& ns_addr, const uint32_t block_id, BLOCK_FILE_INFO_VEC& v_block_file_info)
{
  int ret = TFS_ERROR;
  VUINT64 ds_list;
  GetBlockInfoMessage gbi_message;
  gbi_message.set_block_id(block_id);

  tbnet::Packet* rsp = NULL;
  NewClient* client = NewClientManager::get_instance().create_client();
  ret = send_msg_to_server(Func::get_host_ip(ns_addr.c_str()), client, &gbi_message, rsp);

  if (rsp != NULL)
  {
    if (rsp->getPCode() == SET_BLOCK_INFO_MESSAGE)
    {
      ds_list = dynamic_cast<SetBlockInfoMessage*>(rsp)->get_block_ds();
      if (ds_list.size() > 0)
      {
        uint64_t ds_id = ds_list[0];
        TBSYS_LOG(DEBUG, "ds_ip: %s", tbsys::CNetUtil::addrToString(ds_id).c_str());
        GetServerStatusMessage req_gss_msg;
        req_gss_msg.set_status_type(GSS_BLOCK_FILE_INFO);
        req_gss_msg.set_return_row(block_id);

        int ret_status = TFS_ERROR;
        tbnet::Packet* ret_msg = NULL;
        NewClient* new_client = NewClientManager::get_instance().create_client();
        ret_status = send_msg_to_server(ds_id, new_client, &req_gss_msg, ret_msg);

        //if the information of file can be accessed.
        if ((ret_status == TFS_SUCCESS))
        {
          TBSYS_LOG(DEBUG, "ret_msg->pCODE: %d", ret_msg->getPCode());
          if (BLOCK_FILE_INFO_MESSAGE == ret_msg->getPCode())
          {
            FILE_INFO_LIST* file_info_list = (dynamic_cast<BlockFileInfoMessage*> (ret_msg))->get_fileinfo_list();
            int32_t i = 0;
            int32_t list_size = file_info_list->size();
            for (i = 0; i < list_size; i++)
            {
              FileInfo& file_info = file_info_list->at(i);
              FSName fsname(block_id, file_info.id_, TfsClientImpl::Instance()->get_cluster_id(ns_addr.c_str()));
              TBSYS_LOG(DEBUG, "block_id: %u, file_id: %"PRI64_PREFIX"u, name: %s\n", block_id, file_info.id_, fsname.get_name());
              v_block_file_info.push_back(BlockFileInfo(block_id, file_info));
            }
          }
          else if (STATUS_MESSAGE == ret_msg->getPCode())
          {
            printf("%s", (dynamic_cast<StatusMessage*> (ret_msg))->get_error());
          }
        }
        else
        {
          fprintf(stderr, "Get File list in Block failure\n");
        }
        NewClientManager::get_instance().destroy_client(new_client);
      }
    }
    else if (rsp->getPCode() == STATUS_MESSAGE)
    {
      ret = dynamic_cast<StatusMessage*>(rsp)->get_status();
      fprintf(stderr, "get block info from %s fail, error: %s\n", ns_addr.c_str(), dynamic_cast<StatusMessage*>(rsp)->get_error());
    }
  }
  else
  {
    fprintf(stderr, "get NULL response message, ret: %d\n", ret);
  }

  NewClientManager::get_instance().destroy_client(client);

  return ret;
}
int sync_file(const string& src_ns_addr, const string& dest_ns_addr, const uint32_t block_id, FileInfo& file_info,
    const bool force, const int32_t modify_time)
{
  int ret = TFS_SUCCESS;

  SyncFileBase sync_op;
  sync_op.initialize(src_ns_addr, dest_ns_addr);
  SyncAction sync_action;

  // compare file info
  ret = sync_op.cmp_file_info(block_id, file_info, sync_action, force, modify_time);

  FSName fsname(block_id, file_info.id_, TfsClientImpl::Instance()->get_cluster_id(src_ns_addr.c_str()));
  string file_name = string(fsname.get_name());
  // do sync file
  if (ret == TFS_SUCCESS)
  {
    ret = sync_op.do_action(file_name, sync_action);
  }
  if (ret == TFS_SUCCESS)
  {
    if (sync_action.size() == 0)
    {
      TBSYS_LOG(DEBUG, "sync file(%s) succeed without action.", file_name.c_str());
      {
        tbutil::Mutex::Lock lock(g_mutex_);
        g_sync_stat_.unsync_count_++;
      }
      fprintf(g_file_unsync, "%s\n", file_name.c_str());
    }
    else
    {
      TBSYS_LOG(DEBUG, "sync file(%s) succeed.", file_name.c_str());
      {
        tbutil::Mutex::Lock lock(g_mutex_);
        g_sync_stat_.success_count_++;
      }
      fprintf(g_file_succ, "%s\n", file_name.c_str());
    }
  }
  else
  {
    TBSYS_LOG(DEBUG, "sync file(%s) failed.", file_name.c_str());
    {
      tbutil::Mutex::Lock lock(g_mutex_);
      g_sync_stat_.fail_count_++;
    }
    fprintf(g_file_fail, "%s\n", file_name.c_str());
  }
  g_sync_stat_.actual_count_++;

  return ret;
}
