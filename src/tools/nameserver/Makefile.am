AM_CPPFLAGS=-I$(top_srcdir)/src\
      $(TAIR_CACHE_CPPFLAGS)\
	    -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet\
	    $(READLINE_INCLUDE)

AM_LDFLAGS=-lz -lrt -lpthread -ldl $(READLINE_LIB) -luuid

bin_PROGRAMS = admintool showsyncoplog rmsyncoplog ssm tfstool performance syncbyfile\
               read_syncoplog_header repair_block_info
if WITH_TAIR_CACHE
bin_PROGRAMS += remote_block_cache_tool
endif

LDADD = $(top_builddir)/src/tools/util/libtfstoolsutil.a\
	$(top_builddir)/src/dataserver/libdataserver.a\
	$(top_builddir)/src/new_client/.libs/libtfsclient.a\
	$(top_builddir)/src/message/libtfsmessage.a\
	$(top_builddir)/src/common/libtfscommon.a\
	$(UNIQUE_STORE_LDFLAGS)\
  $(TAIR_CACHE_LDFLAGS) \
	$(TBLIB_ROOT)/lib/libtbnet.a\
	$(TBLIB_ROOT)/lib/libtbsys.a

showsyncoplog_SOURCES = showsyncoplog.cpp

tfstool_SOURCES =  tfstool.cpp

ssm_SOURCES = main.cpp show.cpp show_factory.cpp metacmp.cpp cmp_factory.cpp common.cpp\
							show.h  show_factory.h metacmp.h cmp_factory.h common.h

rmsyncoplog_SOURCES=rmsyncoplog.cpp
read_syncoplog_header_SOURCES=read_syncoplog_header.cpp

admintool_SOURCES = admintool.cpp
performance_SOURCES = performance.cpp
syncbyfile_SOURCES=sync_by_file.cpp
repair_block_info_SOURCES=repair_block_info.cpp
if WITH_TAIR_CACHE
remote_block_cache_tool_SOURCES=remote_block_cache_tool.cpp
endif
