OPENRESTY_PREFIX=/usr/local/stor-openresty/nginx

INSTALL_PREFIX=
INSTALL ?= install
PROXY_DIR = $(OPENRESTY_PREFIX)/proxy
LIB_DIR = /usr/lib64
.PHONY: all test install

all: ;

install: all
	$(INSTALL) -d $(INSTALL_PREFIX)${LIB_DIR}/
	$(INSTALL) -d $(INSTALL_PREFIX)$(PROXY_DIR)
	$(INSTALL) -d $(INSTALL_PREFIX)$(OPENRESTY_PREFIX)/conf/
	$(INSTALL) nginx.conf $(INSTALL_PREFIX)$(OPENRESTY_PREFIX)/conf/
	cp -r * $(INSTALL_PREFIX)$(PROXY_DIR)/
	#sed -i -e "s/\"node1.localdomain\"/\"$$HOSTNAME\"/g" $(INSTALL_PREFIX)$(PROXY_DIR)/storageproxy_conf.lua
	ln -sf $(PROXY_DIR)/thirdparty/thrift/lib/liblualongnumber.so.0 $(INSTALL_PREFIX)${LIB_DIR}/liblualongnumber.so.0
	ln -sf $(PROXY_DIR)/thirdparty/hash/libhash.so $(INSTALL_PREFIX)${LIB_DIR}/libhash.so
	
test: all
	PATH=$(INSTALL_PREFIX)$(OPENRESTY_PREFIX)/sbin:$$PATH prove -I../test-nginx/lib -r t

