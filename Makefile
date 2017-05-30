PROJECT = cloudQ
PROJECT_DESCRIPTION = Erlang lib to connect to different cloud queues.
PROJECT_VERSION = 0.1.0

BUILD_DEPS  = reload_mk
DEP_PLUGINS = reload_mk

DEPS = erlcloud poolboy lager hackney jch brod
dep_jch = git https://github.com/darach/jch-erl master

LOCAL_DEPS = observer runtime_tools wx

include erlang.mk


fast-reload: fast
	$(MAKE) reload SKIP_DEPS=1

fast:
	$(MAKE) SKIP_DEPS=1

console:
	./_rel/cloudQ_release/bin/cloudQ_release console RELOADABLE=1
