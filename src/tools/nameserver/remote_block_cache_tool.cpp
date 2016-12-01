/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfstool.cpp 1000 2011-11-03 02:40:09Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan <mingyan.zc@taobao.com>
 *      - initial release
 *
 */
#include <stdio.h>
#include <pthread.h>
#include <signal.h>

#include <vector>
#include <string>
#include <map>

#include "tbsys.h"

#include "common/internal.h"
#include "common/config_item.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "tools/util/tool_util.h"
#include "new_client/tfs_client_impl.h"

using namespace std;
using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::tools;

static TfsClientImpl* g_tfs_client = NULL;
static STR_FUNC_MAP g_cmd_map;


#ifdef _WITH_READ_LINE
#include "readline/readline.h"
#include "readline/history.h"

char* match_cmd(const char* text, int state)
{
  static STR_FUNC_MAP_ITER it;
  static int len = 0;
  const char* cmd = NULL;

  if (!state)
  {
    it = g_cmd_map.begin();
    len = strlen(text);
  }

  while(it != g_cmd_map.end())
  {
    cmd = it->first.c_str();
    it++;
    if (strncmp(cmd, text, len) == 0)
    {
      int32_t cmd_len = strlen(cmd) + 1;
      // memory will be freed by readline
      return strncpy(new char[cmd_len], cmd, cmd_len);
    }
  }
  return NULL;
}

char** tfscmd_completion (const char* text, int start, int)
{
  rl_attempted_completion_over = 1;
  // at the start of line, then it's a cmd completion
  return (0 == start) ? rl_completion_matches(text, match_cmd) : (char**)NULL;
}
#endif

static void sign_handler(const int32_t sig);
static void usage(const char* name);
void init();
int main_loop();
int do_cmd(char* buffer);

int cmd_show_help(const VSTRING& param);
int cmd_quit(const VSTRING& param);
int cmd_batch_file(const VSTRING& param);
int cmd_insert_block_cache(const VSTRING& param);
int cmd_lookup_block_cache(const VSTRING& param);
int cmd_remove_block_cache(const VSTRING& param);

const char* g_ns_addr = NULL;
const char* g_tair_master_addr = NULL;
const char* g_tair_slave_addr = NULL;
const char* g_tair_group_name = NULL;
int g_tair_area = 0;

int main(int argc, char* argv[])
{
  int32_t i;
  int ret = TFS_SUCCESS;
  bool directly = false;
  char *conf_file = NULL;
  bool set_log_level = false;

  // analyze arguments
  while ((i = getopt(argc, argv, "f:nih")) != EOF)
  {
    switch (i)
    {
      case 'n':
        set_log_level = true;
        break;
      case 'f':
        conf_file = optarg;
        break;
      case 'i':
        directly = true;
        break;
      case 'h':
      default:
        usage(argv[0]);
        return TFS_ERROR;
    }
  }

  if (NULL == conf_file)
  {
    usage(argv[0]);
    return TFS_ERROR;
  }

  if (set_log_level)
  {
    TBSYS_LOGGER.setLogLevel("ERROR");
  }

  TBSYS_CONFIG.load(conf_file);
  g_ns_addr = TBSYS_CONFIG.getString("public", "ns_addr", NULL);
  g_tair_master_addr = TBSYS_CONFIG.getString("public", "tair_master_addr", NULL);
  g_tair_slave_addr = TBSYS_CONFIG.getString("public", "tair_slave_addr", NULL);
  g_tair_group_name = TBSYS_CONFIG.getString("public", "tair_group_name", NULL);
  g_tair_area = TBSYS_CONFIG.getInt("public", "tair_area", 0);
  //file_list = TBSYS_CONFIG.getString("public", "file_list", "./file_to_invalid_remote_cache.list");

  if (NULL == g_ns_addr ||
      NULL == g_tair_master_addr || NULL == g_tair_slave_addr ||
      NULL == g_tair_group_name || g_tair_area < 0)
  {
     fprintf(stderr, "error! must config ns addr and remote cache info!\n");
     return ret;
  }

  init();

  // init tfs client
  g_tfs_client = TfsClientImpl::Instance();
  ret = g_tfs_client->initialize(g_ns_addr, DEFAULT_BLOCK_CACHE_TIME, DEFAULT_BLOCK_CACHE_ITEMS, false);
  if (TFS_SUCCESS != ret)
  {
    fprintf(stderr, "init tfs client fail, ret: %d\n", ret);
    return ret;
  }

  // set remote cache info
  g_tfs_client->set_remote_cache_info(g_tair_master_addr, g_tair_slave_addr,
                                      g_tair_group_name, g_tair_area);
  g_tfs_client->set_use_local_cache();
  g_tfs_client->set_use_remote_cache();

  if (optind >= argc)
  {
    signal(SIGINT, sign_handler);
    signal(SIGTERM, sign_handler);
    main_loop();
  }
  else // has other params
  {
    int32_t i = 0;
    if (directly)
    {
      for (i = optind; i < argc; i++)
      {
        do_cmd(argv[i]);
      }
    }
    else
    {
      VSTRING param;
      for (i = optind; i < argc; i++)
      {
        param.clear();
        param.push_back(argv[i]);
        cmd_batch_file(param);
      }
    }
  }
  if (g_tfs_client != NULL)
  {
    g_tfs_client->destroy();
  }
  return TFS_SUCCESS;
}

static void usage(const char* name)
{
  fprintf(stderr,
          "Usage: a) %s -f config_file [-n] [-i] [-h]. \n"
          "       -f config file\n"
          "       -n set log level\n"
          "       -i directly execute the command\n"
          "       -h help\n",
          name);
}

static void sign_handler(const int32_t sig)
{
  switch (sig)
  {
  case SIGINT:
  case SIGTERM:
    fprintf(stderr, "\nTFS> ");
      break;
  }
}

void init()
{
  g_cmd_map["help"] = CmdNode("help", "show help info", 0, 0, cmd_show_help);
  g_cmd_map["quit"] = CmdNode("quit", "quit", 0, 0, cmd_quit);
  g_cmd_map["exit"] = CmdNode("exit", "exit", 0, 0, cmd_quit);
  g_cmd_map["@"] = CmdNode("@ file", "batch run command in file", 1, 1, cmd_batch_file);
  g_cmd_map["batch"] = CmdNode("batch file", "batch run command in file", 1, 1, cmd_batch_file);
  g_cmd_map["insert"] = CmdNode("insert block_id ds_addr1 ds_addr2", "insert block cache", 2, 3, cmd_insert_block_cache);
  g_cmd_map["lookup"] = CmdNode("lookup block_id", "lookup block cache", 1, 1, cmd_lookup_block_cache);
  g_cmd_map["remove"] = CmdNode("remove block_id", "remove block cache", 1, 1, cmd_remove_block_cache);
}

int main_loop()
{
#ifdef _WITH_READ_LINE
  char* cmd_line = NULL;
  rl_attempted_completion_function = tfscmd_completion;
#else
  char cmd_line[MAX_CMD_SIZE];
#endif

  int ret = TFS_ERROR;
  while (1)
  {
#ifdef _WITH_READ_LINE
    cmd_line = readline("TFS> ");
    if (!cmd_line)
#else
      fprintf(stdout, "TFS> ");
    if (NULL == fgets(cmd_line, MAX_CMD_SIZE, stdin))
#endif
    {
      continue;
    }
    ret = do_cmd(cmd_line);
#ifdef _WITH_READ_LINE
    free(cmd_line);
#endif
    if (TFS_CLIENT_QUIT == ret)
    {
      break;
    }
  }
  return TFS_SUCCESS;
}

int32_t do_cmd(char* key)
{
  char* token;
  while (' ' == *key)
  {
    key++;
  }
  token = key + strlen(key);
  while (' ' == *(token - 1) || '\n' == *(token - 1) || '\r' == *(token - 1))
  {
    token--;
  }
  *token = '\0';
  if ('\0' == key[0])
  {
    return TFS_SUCCESS;
  }

#ifdef _WITH_READ_LINE
  // not blank line, add to history
  add_history(key);
#endif

  token = strchr(key, ' ');
  if (token != NULL)
  {
    *token = '\0';
  }

  string cur_cmd = Func::str_to_lower(key);
  STR_FUNC_MAP_ITER it = g_cmd_map.find(cur_cmd);

  if (it == g_cmd_map.end())
  {
    fprintf(stderr, "unknown command. \n");
    return TFS_ERROR;
  }

  if (token != NULL)
  {
    token++;
    key = token;
  }
  else
  {
    key = NULL;
  }

  VSTRING param;
  param.clear();
  while ((token = strsep(&key, " ")) != NULL)
  {
    if ('\0' == token[0])
    {
      continue;
    }
    param.push_back(token);
  }

  // check param count
  int32_t param_cnt = param.size();
  if (param_cnt < it->second.min_param_cnt_ || param_cnt > it->second.max_param_cnt_)
  {
    fprintf(stderr, "%s\t\t%s\n\n", it->second.syntax_, it->second.info_);
    return TFS_ERROR;
  }

  return it->second.func_(param);
}

const char* canonical_param(const string& param)
{
  const char* ret_param = param.c_str();
  if (NULL != ret_param &&
      (strlen(ret_param) == 0 ||
       strcasecmp(ret_param, "null") == 0))
  {
    ret_param = NULL;
  }
  return ret_param;
}

// expand ~ to HOME. modify argument
const char* expand_path(string& path)
{
  if (path.size() > 0 && '~' == path.at(0) &&
      (1 == path.size() ||                      // just one ~
       (path.size() > 1 && '/' == path.at(1)))) // like ~/xxx
  {
    char* home_path = getenv("HOME");
    if (NULL == home_path)
    {
      fprintf(stderr, "can't get HOME path: %s\n", strerror(errno));
    }
    else
    {
      path.replace(0, 1, home_path);
    }
  }
  return path.c_str();
}

int cmd_insert_block_cache(const VSTRING& param)
{
  size_t size = param.size();
  uint32_t ds_count = size - 1;
  uint32_t block_id = atoi(param[0].c_str());
  VUINT64 ds_list;
  for (size_t i = 0; i < ds_count; i++)
  {
    ds_list.push_back(Func::get_host_ip(param[i + 1].c_str()));
  }
  if (ds_list.empty())
  {
    return TFS_ERROR;
  }
  g_tfs_client->insert_remote_block_cache(NULL, block_id, ds_list);
  return TFS_SUCCESS;
}

int cmd_lookup_block_cache(const VSTRING& param)
{
  uint32_t block_id = atoi(param[0].c_str());
  VUINT64 ds_list;
  int ret = g_tfs_client->query_remote_block_cache(NULL, block_id, ds_list);
  ToolUtil::print_info(ret, "lookup block cache");
  for (size_t i = 0; i < ds_list.size(); i++)
  {
    fprintf(stdout, "ds_addr: %s \n", tbsys::CNetUtil::addrToString(ds_list[i]).c_str());
  }
  return TFS_SUCCESS;
}

int cmd_remove_block_cache(const VSTRING& param)
{
  int32_t block_id = atoi(param[0].c_str());
  g_tfs_client->remove_remote_block_cache(NULL, block_id);
  return TFS_SUCCESS;
}

int cmd_quit(const VSTRING&)
{
  return TFS_CLIENT_QUIT;
}

int cmd_show_help(const VSTRING&)
{
  return ToolUtil::show_help(g_cmd_map);
}

int cmd_batch_file(const VSTRING& param)
{
  const char* batch_file = expand_path(const_cast<string&>(param[0]));
  FILE* fp = fopen(batch_file, "rb");
  int ret = TFS_SUCCESS;
  if (fp == NULL)
  {
    fprintf(stderr, "open file error: %s\n\n", batch_file);
    ret = TFS_ERROR;
  }
  else
  {
    int32_t error_count = 0;
    int32_t count = 0;
    VSTRING params;
    char buffer[MAX_CMD_SIZE];
    while (fgets(buffer, MAX_CMD_SIZE, fp))
    {
      if ((ret = do_cmd(buffer)) == TFS_ERROR)
      {
        error_count++;
      }
      if (++count % 100 == 0)
      {
        fprintf(stdout, "tatol: %d, %d errors.\r", count, error_count);
        fflush(stdout);
      }
      if (TFS_CLIENT_QUIT == ret)
      {
        break;
      }
    }
    fprintf(stdout, "tatol: %d, %d errors.\n\n", count, error_count);
    fclose(fp);
  }
  return TFS_SUCCESS;
}

