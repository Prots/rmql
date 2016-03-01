PROJECT = rmql

DEPS = lager amqp_client

dep_amqp_client = git https://github.com/rabbitmq/rabbitmq-erlang-client.git	rabbitmq_v3_6_0
dep_lager       = git https://github.com/basho/lager.git            		    3.1.0

# this must be first
include erlang.mk

# Compile flags
ERLC_COMPILE_OPTS= +'{parse_transform, lager_transform}'

# Append these settings
ERLC_OPTS += $(ERLC_COMPILE_OPTS)
TEST_ERLC_OPTS += $(ERLC_COMPILE_OPTS)