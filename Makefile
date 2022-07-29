.PHONY: compile rel cover test dialyzer
REBAR=./rebar3

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean
	@rm src/riak_core_pb.erl

cover: test
	$(REBAR) cover

test: compile
	$(REBAR) as test do eunit
	@rm -rf riak_kv_compile_tab*.dets undefined/* data.* log.*

dialyzer:
	$(REBAR) dialyzer

xref:
	$(REBAR) xref

check: test dialyzer xref
