local _M = {}
--import external 
local ffi = require "ffi"
local C = ffi.C
local tonumber = tonumber
local tostring = tostring
local print = print
--package.path = package.path .. ';/home/deploy/dobjstor/proxy/thirdparty/?.lua'
--local package_path="/home/deploy/dobjstor/proxy/?.lua;/home/deploy/dobjstor/proxy/thirdparty/?.lua;/home/deploy/luas3/?.lua;;"
local inspect = require "inspect"
ffi.cdef[[
typedef void *time_t;
struct rados_cluster_stat_t {
  uint64_t kb, kb_used, kb_avail;
    uint64_t num_objects;
    };
typedef void *rados_t;
typedef void *rados_config_t;
typedef void *rados_ioctx_t;
typedef void *rados_list_ctx_t;
typedef void * rados_object_list_cursor;
typedef struct rados_object_list_item {
                         size_t oid_length;
                         char *oid;
                         size_t nspace_length;
                         char *nspace;
                         size_t locator_length;
                         char *locator;
                        } rados_object_list_item;
typedef uint64_t rados_snap_t;
typedef void *rados_xattrs_iter_t;
typedef void *rados_omap_iter_t;
typedef void *rados_write_op_t;
typedef void *rados_read_op_t;
typedef void *rados_completion_t;
typedef void (*rados_callback_t)(rados_completion_t cb, void *arg);
typedef void (*rados_watchcb_t)(uint8_t opcode, uint64_t ver, void *arg);
typedef void (*rados_watchcb2_t)(void *arg,
                                 uint64_t notify_id,
                                 uint64_t handle,
                                 uint64_t notifier_id,
                                 void *data,
                                 size_t data_len);
typedef void (*rados_watcherrcb_t)(void *pre, uint64_t cookie, int err);
typedef void (*rados_log_callback_t)(void *arg,
                                     const char *line,
                                     const char *who,
                                     uint64_t sec, uint64_t nsec,
                                     uint64_t seq, const char *level,
                                     const char *msg);
void rados_version(int *major, int *minor, int *extra);
int rados_create(rados_t *cluster, const char * const id);
int rados_create2(rados_t *pcluster,
                  const char *const clustername,
                  const char * const name, uint64_t flags);
int rados_create_with_context(rados_t *cluster,
                              rados_config_t cct);
int rados_ping_monitor(rados_t cluster, const char *mon_id,
                       char **outstr, size_t *outstrlen);
int rados_connect(rados_t cluster);
void rados_shutdown(rados_t cluster);
int rados_conf_read_file(rados_t cluster, const char *path);
int rados_conf_parse_argv(rados_t cluster, int argc,
                          const char **argv);
int rados_conf_parse_argv_remainder(rados_t cluster, int argc,
 		                   const char **argv,
                                    const char **remargv);
int rados_conf_parse_env(rados_t cluster, const char *var);
int rados_conf_set(rados_t cluster, const char *option,
                   const char *value);
int rados_conf_get(rados_t cluster, const char *option,
                   char *buf, size_t len);
int rados_cluster_stat(rados_t cluster,
                       struct rados_cluster_stat_t *result);
int rados_cluster_fsid(rados_t cluster, char *buf, size_t len);
int rados_wait_for_latest_osdmap(rados_t cluster);
int rados_pool_list(rados_t cluster, char *buf, size_t len);
int rados_inconsistent_pg_list(rados_t cluster, int64_t pool,
 			      char *buf, size_t len);
rados_config_t rados_cct(rados_t cluster);
uint64_t rados_get_instance_id(rados_t cluster);
int rados_ioctx_create(rados_t cluster, const char *pool_name,
                       rados_ioctx_t *ioctx);
int rados_ioctx_create2(rados_t cluster, int64_t pool_id,
                        rados_ioctx_t *ioctx);
void rados_ioctx_destroy(rados_ioctx_t io);
rados_config_t rados_ioctx_cct(rados_ioctx_t io);
rados_t rados_ioctx_get_cluster(rados_ioctx_t io);
int rados_ioctx_pool_stat(rados_ioctx_t io,
                          struct rados_pool_stat_t *stats);
int64_t rados_pool_lookup(rados_t cluster,
                          const char *pool_name);
int rados_pool_reverse_lookup(rados_t cluster, int64_t id,
                              char *buf, size_t maxlen);
int rados_pool_create(rados_t cluster, const char *pool_name);
int rados_pool_create_with_auid(rados_t cluster,
                                const char *pool_name,
                                uint64_t auid);
int rados_pool_create_with_crush_rule(rados_t cluster,
                                      const char *pool_name,
 		                     uint8_t crush_rule_num);
int rados_pool_create_with_all(rados_t cluster,
                               const char *pool_name,
                               uint64_t auid,
 	                      uint8_t crush_rule_num);
int rados_pool_get_base_tier(rados_t cluster, int64_t pool,
                             int64_t* base_tier);
int rados_pool_delete(rados_t cluster, const char *pool_name);
int rados_ioctx_pool_set_auid(rados_ioctx_t io, uint64_t auid);
int rados_ioctx_pool_get_auid(rados_ioctx_t io, uint64_t *auid);
int rados_ioctx_pool_requires_alignment2(rados_ioctx_t io,
  int *requires);
int rados_ioctx_pool_required_alignment2(rados_ioctx_t io,
  uint64_t *alignment);
int64_t rados_ioctx_get_id(rados_ioctx_t io);
int rados_ioctx_get_pool_name(rados_ioctx_t io, char *buf,
                              unsigned maxlen);
void rados_ioctx_locator_set_key(rados_ioctx_t io,
                                 const char *key);
void rados_ioctx_set_namespace(rados_ioctx_t io,
                               const char *nspace);
int rados_nobjects_list_open(rados_ioctx_t io,
                             rados_list_ctx_t *ctx);
uint32_t rados_nobjects_list_get_pg_hash_position(rados_list_ctx_t ctx);
uint32_t rados_nobjects_list_seek(rados_list_ctx_t ctx,
                                  uint32_t pos);
int rados_nobjects_list_next(rados_list_ctx_t ctx,
                             const char **entry,
                             const char **key,
                             const char **nspace);
void rados_nobjects_list_close(rados_list_ctx_t ctx);
rados_object_list_cursor rados_object_list_begin(rados_ioctx_t io);
rados_object_list_cursor rados_object_list_end(rados_ioctx_t io);
int rados_object_list_is_end(rados_ioctx_t io,
    rados_object_list_cursor cur);
void rados_object_list_cursor_free(rados_ioctx_t io,
    rados_object_list_cursor cur);
int rados_object_list_cursor_cmp(rados_ioctx_t io,
    rados_object_list_cursor lhs, rados_object_list_cursor rhs);
int rados_object_list(rados_ioctx_t io,
    const rados_object_list_cursor start,
    const rados_object_list_cursor finish,
    const size_t result_size,
    const char *filter_buf,
    const size_t filter_buf_len,
    rados_object_list_item *results,
    rados_object_list_cursor *next);
void rados_object_list_free(
    const size_t result_size,
    rados_object_list_item *results);
void rados_object_list_slice(rados_ioctx_t io,
    const rados_object_list_cursor start,
    const rados_object_list_cursor finish,
    const size_t n,
    const size_t m,
    rados_object_list_cursor *split_start,
    rados_object_list_cursor *split_finish);
int rados_objects_list_open(rados_ioctx_t io,
                            rados_list_ctx_t *ctx);
uint32_t rados_objects_list_get_pg_hash_position(rados_list_ctx_t ctx);
uint32_t rados_objects_list_seek(rados_list_ctx_t ctx,
                                 uint32_t pos);
int rados_objects_list_next(rados_list_ctx_t ctx,
                            const char **entry,
                            const char **key);
void rados_objects_list_close(rados_list_ctx_t ctx);
int rados_ioctx_snap_create(rados_ioctx_t io,
                            const char *snapname);
int rados_ioctx_snap_remove(rados_ioctx_t io,
                            const char *snapname);
int rados_ioctx_snap_rollback(rados_ioctx_t io, const char *oid,
                              const char *snapname);
void rados_ioctx_snap_set_read(rados_ioctx_t io,
                               rados_snap_t snap);
int rados_ioctx_selfmanaged_snap_create(rados_ioctx_t io,
                                        rados_snap_t *snapid);
int rados_ioctx_selfmanaged_snap_remove(rados_ioctx_t io,
                                        rados_snap_t snapid);
int rados_ioctx_selfmanaged_snap_rollback(rados_ioctx_t io,
                                          const char *oid,
                                          rados_snap_t snapid);
int rados_ioctx_selfmanaged_snap_set_write_ctx(rados_ioctx_t io,
                                               rados_snap_t seq,
                                               rados_snap_t *snaps,
                                               int num_snaps);
int rados_ioctx_snap_list(rados_ioctx_t io, rados_snap_t *snaps,
                          int maxlen);
int rados_ioctx_snap_lookup(rados_ioctx_t io, const char *name,
                            rados_snap_t *id);
int rados_ioctx_snap_get_name(rados_ioctx_t io, rados_snap_t id,
                              char *name, int maxlen);
int rados_ioctx_snap_get_stamp(rados_ioctx_t io, rados_snap_t id,
                               time_t *t);
uint64_t rados_get_last_version(rados_ioctx_t io);
int rados_write(rados_ioctx_t io, const char *oid,
                const char *buf, size_t len, uint64_t off);
int rados_write_full(rados_ioctx_t io, const char *oid,
                     const char *buf, size_t len);
int rados_clone_range(rados_ioctx_t io, const char *dst,
                      uint64_t dst_off, const char *src,
                      uint64_t src_off, size_t len);
int rados_append(rados_ioctx_t io, const char *oid,
                 const char *buf, size_t len);
int rados_read(rados_ioctx_t io, const char *oid, char *buf,
               size_t len, uint64_t off);
int rados_remove(rados_ioctx_t io, const char *oid);
int rados_trunc(rados_ioctx_t io, const char *oid,
                uint64_t size);
int rados_getxattr(rados_ioctx_t io, const char *o,
                   const char *name, char *buf, size_t len);
int rados_setxattr(rados_ioctx_t io, const char *o,
                   const char *name, const char *buf,
                   size_t len);
int rados_rmxattr(rados_ioctx_t io, const char *o,
                  const char *name);
int rados_getxattrs(rados_ioctx_t io, const char *oid,
                    rados_xattrs_iter_t *iter);
int rados_getxattrs_next(rados_xattrs_iter_t iter,
                         const char **name, const char **val,
                         size_t *len);
void rados_getxattrs_end(rados_xattrs_iter_t iter);
int rados_omap_get_next(rados_omap_iter_t iter,
 	               char **key,
 	               char **val,
 	               size_t *len);
void rados_omap_get_end(rados_omap_iter_t iter);
int rados_stat(rados_ioctx_t io, const char *o, uint64_t *psize,
               time_t *pmtime);
int rados_tmap_update(rados_ioctx_t io, const char *o,
                      const char *cmdbuf, size_t cmdbuflen);
int rados_tmap_put(rados_ioctx_t io, const char *o,
                   const char *buf, size_t buflen);
int rados_tmap_get(rados_ioctx_t io, const char *o, char *buf,
                   size_t buflen);
int rados_exec(rados_ioctx_t io, const char *oid,
               const char *cls, const char *method,
               const char *in_buf, size_t in_len, char *buf,
               size_t out_len);
int rados_aio_create_completion(void *cb_arg,
                                rados_callback_t cb_complete,
                                rados_callback_t cb_safe,
 		               rados_completion_t *pc);
int rados_aio_wait_for_complete(rados_completion_t c);
int rados_aio_wait_for_safe(rados_completion_t c);
int rados_aio_is_complete(rados_completion_t c);
int rados_aio_is_safe(rados_completion_t c);
int rados_aio_wait_for_complete_and_cb(rados_completion_t c);
int rados_aio_wait_for_safe_and_cb(rados_completion_t c);
int rados_aio_is_complete_and_cb(rados_completion_t c);
int rados_aio_is_safe_and_cb(rados_completion_t c);
int rados_aio_get_return_value(rados_completion_t c);
void rados_aio_release(rados_completion_t c);
int rados_aio_write(rados_ioctx_t io, const char *oid,
                    rados_completion_t completion,
                    const char *buf, size_t len, uint64_t off);
int rados_aio_append(rados_ioctx_t io, const char *oid,
                     rados_completion_t completion,
                     const char *buf, size_t len);
int rados_aio_write_full(rados_ioctx_t io, const char *oid,
 	                rados_completion_t completion,
 	                const char *buf, size_t len);
int rados_aio_remove(rados_ioctx_t io, const char *oid,
                     rados_completion_t completion);
int rados_aio_read(rados_ioctx_t io, const char *oid,
                   rados_completion_t completion,
                   char *buf, size_t len, uint64_t off);
int rados_aio_flush(rados_ioctx_t io);
int rados_aio_flush_async(rados_ioctx_t io,
                          rados_completion_t completion);
int rados_aio_stat(rados_ioctx_t io, const char *o,
                   rados_completion_t completion,
                   uint64_t *psize, time_t *pmtime);
int rados_aio_cancel(rados_ioctx_t io,
                     rados_completion_t completion);
int rados_watch2(rados_ioctx_t io, const char *o, uint64_t *cookie,
 		rados_watchcb2_t watchcb,
 		rados_watcherrcb_t watcherrcb,
 		void *arg);
int rados_aio_watch(rados_ioctx_t io, const char *o,
 		   rados_completion_t completion, uint64_t *handle,
 		   rados_watchcb2_t watchcb,
 		   rados_watcherrcb_t watcherrcb,
 		   void *arg);
int rados_watch_check(rados_ioctx_t io, uint64_t cookie);
int rados_unwatch2(rados_ioctx_t io, uint64_t cookie);
int rados_aio_unwatch(rados_ioctx_t io, uint64_t cookie,
                      rados_completion_t completion);
int rados_notify2(rados_ioctx_t io, const char *o,
 		 const char *buf, int buf_len,
 		 uint64_t timeout_ms,
 		 char **reply_buffer, size_t *reply_buffer_len);
int rados_aio_notify(rados_ioctx_t io, const char *o,
                     rados_completion_t completion,
                     const char *buf, int buf_len,
                     uint64_t timeout_ms, char **reply_buffer,
                     size_t *reply_buffer_len);
int rados_notify_ack(rados_ioctx_t io, const char *o,
 		    uint64_t notify_id, uint64_t cookie,
 		    const char *buf, int buf_len);
int rados_watch_flush(rados_t cluster);
int rados_aio_watch_flush(rados_t cluster, rados_completion_t completion);
int rados_cache_pin(rados_ioctx_t io, const char *o);
int rados_cache_unpin(rados_ioctx_t io, const char *o);
int rados_set_alloc_hint(rados_ioctx_t io, const char *o,
                         uint64_t expected_object_size,
                         uint64_t expected_write_size);
rados_write_op_t rados_create_write_op(void);
void rados_release_write_op(rados_write_op_t write_op);
void rados_write_op_set_flags(rados_write_op_t write_op,
                              int flags);
void rados_write_op_assert_exists(rados_write_op_t write_op);
void rados_write_op_assert_version(rados_write_op_t write_op, uint64_t ver);
void rados_write_op_cmpxattr(rados_write_op_t write_op,
                             const char *name,
                             uint8_t comparison_operator,
                             const char *value,
                             size_t value_len);
void rados_write_op_omap_cmp(rados_write_op_t write_op,
 	                    const char *key,
 	                    uint8_t comparison_operator,
 	                    const char *val,
 	                    size_t val_len,
 	                    int *prval);
void rados_write_op_setxattr(rados_write_op_t write_op,
                             const char *name,
                             const char *value,
                             size_t value_len);
void rados_write_op_rmxattr(rados_write_op_t write_op,
                            const char *name);
void rados_write_op_create(rados_write_op_t write_op,
                           int exclusive,
                           const char* category);
void rados_write_op_write(rados_write_op_t write_op,
                          const char *buffer,
                          size_t len,
                          uint64_t offset);
void rados_write_op_write_full(rados_write_op_t write_op,
                               const char *buffer,
                               size_t len);
void rados_write_op_append(rados_write_op_t write_op,
                           const char *buffer,
                           size_t len);
void rados_write_op_remove(rados_write_op_t write_op);
void rados_write_op_truncate(rados_write_op_t write_op,
                             uint64_t offset);
void rados_write_op_zero(rados_write_op_t write_op,
 	                uint64_t offset,
 	                uint64_t len);
void rados_write_op_exec(rados_write_op_t write_op,
 	                const char *cls,
 	                const char *method,
 	                const char *in_buf,
 	                size_t in_len,
 	                int *prval);
void rados_write_op_omap_set(rados_write_op_t write_op,
 	                    char const* const* keys,
 	                    char const* const* vals,
 	                    const size_t *lens,
 	                    size_t num);
void rados_write_op_omap_rm_keys(rados_write_op_t write_op,
 		                char const* const* keys,
 		                size_t keys_len);
void rados_write_op_omap_clear(rados_write_op_t write_op);
void rados_write_op_set_alloc_hint(rados_write_op_t write_op,
                                   uint64_t expected_object_size,
                                   uint64_t expected_write_size);
int rados_write_op_operate(rados_write_op_t write_op,
 	                  rados_ioctx_t io,
 	                  const char *oid,
 	                  time_t *mtime,
 	                  int flags);
int rados_write_op_operate2(rados_write_op_t write_op,
                            rados_ioctx_t io,
                            const char *oid,
                            struct timespec *mtime,
                            int flags);
int rados_aio_write_op_operate(rados_write_op_t write_op,
                               rados_ioctx_t io,
                               rados_completion_t completion,
                               const char *oid,
                               time_t *mtime,
 	                      int flags);
rados_read_op_t rados_create_read_op(void);
void rados_release_read_op(rados_read_op_t read_op);
void rados_read_op_set_flags(rados_read_op_t read_op, int flags);
void rados_read_op_assert_exists(rados_read_op_t read_op);
void rados_read_op_assert_version(rados_read_op_t read_op, uint64_t ver);
void rados_read_op_cmpxattr(rados_read_op_t read_op,
 	                   const char *name,
 	                   uint8_t comparison_operator,
 	                   const char *value,
 	                   size_t value_len);
void rados_read_op_getxattrs(rados_read_op_t read_op,
 	                    rados_xattrs_iter_t *iter,
 	                    int *prval);
void rados_read_op_omap_cmp(rados_read_op_t read_op,
 	                   const char *key,
 	                   uint8_t comparison_operator,
 	                   const char *val,
 	                   size_t val_len,
 	                   int *prval);
void rados_read_op_stat(rados_read_op_t read_op,
 	               uint64_t *psize,
 	               time_t *pmtime,
 	               int *prval);
void rados_read_op_read(rados_read_op_t read_op,
 	               uint64_t offset,
 	               size_t len,
 	               char *buffer,
 	               size_t *bytes_read,
 	               int *prval);
void rados_read_op_exec(rados_read_op_t read_op,
 	               const char *cls,
 	               const char *method,
 	               const char *in_buf,
 	               size_t in_len,
 	               char **out_buf,
                        size_t *out_len,
                        int *prval);

void rados_read_op_exec_user_buf(rados_read_op_t read_op,
 		                const char *cls,
 		                const char *method,
 		                const char *in_buf,
 		                size_t in_len,
 		                char *out_buf,
                                 size_t *used_len,
                                 int *prval);
void rados_read_op_omap_get_vals(rados_read_op_t read_op,
 		                const char *start_after,
 		                const char *filter_prefix,
 		                uint64_t max_return,
 		                rados_omap_iter_t *iter,
 		                int *prval);
void rados_read_op_omap_get_keys(rados_read_op_t read_op,
 		                const char *start_after,
 		                uint64_t max_return,
 		                rados_omap_iter_t *iter,
 		                int *prval);
void rados_read_op_omap_get_vals_by_keys(rados_read_op_t read_op,
 			                char const* const* keys,
 			                size_t keys_len,
 			                rados_omap_iter_t *iter,
 			                int *prval);
int rados_read_op_operate(rados_read_op_t read_op,
 	                 rados_ioctx_t io,
 	                 const char *oid,
 	                 int flags);
int rados_aio_read_op_operate(rados_read_op_t read_op,
 	                     rados_ioctx_t io,
 	                     rados_completion_t completion,
 	                     const char *oid,
 	                     int flags);
int rados_lock_exclusive(rados_ioctx_t io, const char * oid,
                         const char * name, const char * cookie,
                         const char * desc,
                         struct timeval * duration,
                         uint8_t flags);
int rados_lock_shared(rados_ioctx_t io, const char * o,
                      const char * name, const char * cookie,
                      const char * tag, const char * desc,
                      struct timeval * duration, uint8_t flags);
int rados_unlock(rados_ioctx_t io, const char *o,
                 const char *name, const char *cookie);
ssize_t rados_list_lockers(rados_ioctx_t io, const char *o,
 	                  const char *name, int *exclusive,
 	                  char *tag, size_t *tag_len,
 	                  char *clients, size_t *clients_len,
 	                  char *cookies, size_t *cookies_len,
 	                  char *addrs, size_t *addrs_len);
int rados_break_lock(rados_ioctx_t io, const char *o,
                     const char *name, const char *client,
                     const char *cookie);
int rados_blacklist_add(rados_t cluster,
 		       char *client_address,
 		       uint32_t expire_seconds);
int rados_mon_command(rados_t cluster, const char **cmd,
                      size_t cmdlen, const char *inbuf,
                      size_t inbuflen, char **outbuf,
                      size_t *outbuflen, char **outs,
                      size_t *outslen);
int rados_mon_command_target(rados_t cluster, const char *name,
 	                    const char **cmd, size_t cmdlen,
 	                    const char *inbuf, size_t inbuflen,
 	                    char **outbuf, size_t *outbuflen,
 	                    char **outs, size_t *outslen);
void rados_buffer_free(char *buf);
int rados_osd_command(rados_t cluster, int osdid,
                      const char **cmd, size_t cmdlen,
                      const char *inbuf, size_t inbuflen,
                      char **outbuf, size_t *outbuflen,
                      char **outs, size_t *outslen);
int rados_pg_command(rados_t cluster, const char *pgstr,
                     const char **cmd, size_t cmdlen,
                     const char *inbuf, size_t inbuflen,
                     char **outbuf, size_t *outbuflen,
                     char **outs, size_t *outslen);
int rados_monitor_log(rados_t cluster, const char *level,
                      rados_log_callback_t cb, void *arg);
]]

local librados = ffi.load("rados")
_ENV = _M


--/* Initialize the cluster handle with the "ceph" cluster name and the "client.admin" user */
function _M.rds_createorigin(cluster_name,user_name,flags)
        local cluster_name = ffi.new("char[?]",#cluster_name, cluster_name)
        local user_name = ffi.new("char[?]", #user_name,user_name)
        local flags = ffi.new("uint64_t",flags)
        local err = ffi.new("int")
--	print("cluster_name is ", cluster_name)
        local clusterh = ffi.new("rados_t[1]")
        err = librados.rados_create2(clusterh, cluster_name, user_name, flags);


        if (tonumber(err) < 0) then
         ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "Couldn't create the cluster handle! \n");
        else 
         ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "\nCreated a cluster handle.\n");
        end
	return err,clusterh
end
--/* Initialize the cluster handle with the "ceph" cluster name and the "client.admin" user */
function _M.rds_create2(clusterh,cluster_name,user_name,flags)
        local cluster_name = ffi.new("char[?]",#cluster_name, cluster_name)
        local user_name = ffi.new("char[?]", #user_name,user_name)
        local flags = ffi.new("uint64_t",flags)
        local err = ffi.new("int")
--	print("cluster_name is ", cluster_name)
        err = librados.rados_create2(clusterh, cluster_name, user_name, flags);


        if (tonumber(err) < 0) then
         ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "Couldn't create the cluster handle! \n");
        else 
         ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "\nCreated a cluster handle.\n");
        end
	return err
end
--[[/**
 * Configure the cluster handle using a Ceph config file
 *
 * If path is NULL, the default locations are searched, and the first
 * found is used. The locations are:
 * - $CEPH_CONF (environment variable)
 * - /etc/ceph/ceph.conf
 * - ~/.ceph/config
 * - ceph.conf (in the current working directory)
 *
 * @pre rados_connect() has not been called on the cluster handle
 *
 * @param clusterh cluster handle to configure
 * @param path path to a Ceph configuration file
 * @returns 0 on success, negative error code on failure
 */
CEPH_RADOS_API int rados_conf_read_file(rados_t cluster, const char *path);
]]
function _M.rds_conf_read_file(clusterh, path)
         local err = ffi.new("int")
         --err = librados.rados_conf_read_file(clusterh,path)
--	 err = ffi.errno 
         if path == nil then 
        	 path = ffi.new("void *",nil)
         end
-- path should be const char.	 
        err = librados.rados_conf_read_file(clusterh[0],path)
        -- err = librados.rados_conf_read_file(clusterh,ffi.new("void *",nil))
--	 print(tostring(err))
         print("read conf err is ",inspect(err))

         if (err < 0) then
         ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "cannot read config file" .. path .. "\n")
                return err
         else 
         ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "readed config file success,config path is " .. path .. "\n")
		return err
         end
end
--[[
/**
 * Connect to the cluster.
 *
 * @note BUG: Before calling this, calling a function that communicates with the
 * cluster will crash.
 *
 * @pre The cluster handle is configured with at least a monitor
 * address. If cephx is enabled, a client name and secret must also be
 * set.
 *
 * @post If this succeeds, any function in librados may be used
 *
 * @param cluster The cluster to connect to.
 * @returns 0 on sucess, negative error code on failure
 */
CEPH_RADOS_API int rados_connect(rados_t cluster);

]]
function _M.rados_connect(clusterh)
         local err = ffi.new("int")
         err = librados.rados_connect(clusterh[0])
         print("connect err is ",inspect(err))
         print("connect err is in orgin ",tostring(err))
         return err
end        
--[[

/**
 * Read usage info about the cluster
 *
 * This tells you total space, space used, space available, and number
 * of objects. These are not updated immediately when data is written,
 * they are eventually consistent.
 *
 * @param cluster cluster to query
 * @param result where to store the results
 * @returns 0 on success, negative error code on failure
 */
CEPH_RADOS_API int rados_cluster_stat(rados_t cluster,
                                      struct rados_cluster_stat_t *result);
]]
function _M.rados_cluster_stat(clusterh, clusterstat)
         local stat_type = ffi.typeof("struct rados_cluster_stat_t *")
	 local stat_size = ffi.sizeof("struct rados_cluster_stat_t")
	 print("size is ",stat_size)
--	 clusterstat = stat_type({})
--         clusterstat= ffi.new(stat_type[1],nil)
--         clusterstat= ffi.new(stat_type)

         clusterstat = ffi.cast(stat_type,ffi.new("char[?]",stat_size))
         local err = ffi.new("int")
         err = librados.rados_cluster_stat(clusterh[0],clusterstat[0])
         print("get cluster stat  err is ",inspect(err))
	 print(clusterstat.kb, clusterstat.kb_used, clusterstat.kb_avail, clusterstat.num_objects)

	 -- cdata object need be tranfered to lua object
	 local stat = {tonumber(clusterstat.kb), tonumber(clusterstat.kb_used), tonumber(clusterstat.kb_avail), tonumber(clusterstat.num_objects)}
--[[second method
         local clusterstat = ffi.new("struct rados_cluster_stat_t [1]")

         err = librados.rados_cluster_stat(clusterh[0],clusterstat)
	 local stat = {tonumber(clusterstat[0].kb), tonumber(clusterstat[0].kb_used), tonumber(clusterstat[0].kb_avail), tonumber(clusterstat[0].num_objects)}
]]
         return err,stat
end
--[[
/**
 * List pools
 *
 * Gets a list of pool names as NULL-terminated strings.  The pool
 * names will be placed in the supplied buffer one after another.
 * After the last pool name, there will be two 0 bytes in a row.
 *
 * If len is too short to fit all the pool name entries we need, we will fill
 * as much as we can.
 *
 * @param cluster cluster handle
 * @param buf output buffer
 * @param len output buffer length
 * @returns length of the buffer we would need to list all pools
 */
CEPH_RADOS_API int rados_pool_list(rados_t cluster, char *buf, size_t len);


]]

function _M.rados_pool_list(clusterh)
         local buf = ffi.new("char[?]",1000)
         local len = ffi.new("size_t",1000)
         local len1 = ffi.new("int")
-- clusterh should not addr, instead value
         len1 = librados.rados_pool_list(clusterh[0],buf,len)
         print("list pools buf len1 is ",tonumber(len1))
         local pools = ffi.string(buf,len1)
--	 local size = ffi.sizeof(buf)
--	 print("buf size",size)
         return pools
end        
--[[
/**
 * Create a pool with default settings
 *
 * The default owner is the admin user (auid 0).
 * The default crush rule is rule 0.
 *
 * @param cluster the cluster in which the pool will be created
 * @param pool_name the name of the new pool
 * @returns 0 on success, negative error code on failure
 */
CEPH_RADOS_API int rados_pool_create(rados_t cluster, const char *pool_name);

]]
function _M.rados_pool_create(clusterh,poolname)
         local err = ffi.new("int")
         err = librados.rados_pool_create(clusterh[0],poolname)
         return err  
end
--[[
/**
 * Create an io context
 *
 * The io context allows you to perform operations within a particular
 * pool. For more details see rados_ioctx_t.
 *
 * @param cluster which cluster the pool is in
 * @param pool_name name of the pool
 * @param ioctx where to store the io context
 * @returns 0 on success, negative error code on failure
 */
CEPH_RADOS_API int rados_ioctx_create(rados_t cluster, const char *pool_name,
                                      rados_ioctx_t *ioctx);
CEPH_RADOS_API int rados_ioctx_create2(rados_t cluster, int64_t pool_id,
                                       rados_ioctx_t *ioctx);
    
]]

function _M.rados_ioctx_create(clusterh,poolname)
         local ioctx = ffi.new("rados_ioctx_t[1]")       
         local err = ffi.new("int")
         err=librados.rados_ioctx_create(clusterh[0],poolname,ioctx)
         --shutdown clusterh

         print("ioctx err is ",inspect(err))
	 return err,ioctx
end        
--[[
/**
 * Write *len* bytes from *buf* into the *oid* object, starting at
 * offset *off*. The value of *len* must be <= UINT_MAX/2.
 *
 * @note This will never return a positive value not equal to len.
 * @param io the io context in which the write will occur
 * @param oid name of the object
 * @param buf data to write
 * @param len length of the data, in bytes
 * @param off byte offset in the object to begin writing at
 * @returns 0 on success, negative error code on failure
 */
CEPH_RADOS_API int rados_write(rados_ioctx_t io, const char *oid,
                               const char *buf, size_t len, uint64_t off);

/**
 * Write *len* bytes from *buf* into the *oid* object. The value of
 * *len* must be <= UINT_MAX/2.
 *
 * The object is filled with the provided data. If the object exists,
 * it is atomically truncated and then written.
 *
 * @param io the io context in which the write will occur
 * @param oid name of the object
 * @param buf data to write
 * @param len length of the data, in bytes
 * @returns 0 on success, negative error code on failure
 */
rados_write_full
]]

--for write object
function _M.rados_write(ioctx,oid,buf,len,off)
         local err = ffi.new("int")
         local len1 = ffi.new("size_t",len)
         err = librados.rados_write(ioctx[0],oid,buf,len,off)

         if err < 0 then
         print "write object fail"
         --ioctxdestory
         --shutdown cluster
         end
end
--[[
int rados_read(rados_ioctx_t io, const char *oid, char *buf,
                              size_t len, uint64_t off);
rados_write_full
rados_remove
]]			      
function _M.rados_read(ioctx,oid,len,off)
	 --local err = ffi.new("int")
	 local err
         local len1 = ffi.new("size_t",len)
	 local buf = ffi.new("char[?]",len) --malloc alanative?
	 local oid1 = ffi.cast("const char *",oid)
	 ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ", "oid is ",oid,"len is ",len)
         err = librados.rados_read(ioctx[0],oid1,buf,len1,off)
	 if err < 0 then
	       do 
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " rados read err code is ", tonumber(err))
	        --ioctxdestory
                --shutdown cluster
	       return err 
               end
         else 
	    --  ffi.string should use cdata 
	       local luabuf = ffi.string(buf,err)
            -- print("read buf is" ,luabuf)
	      local len = err
	      return len,luabuf

	 end
end
--[[
Delete an object

@note This does not delete any snapshots of the object.

@param io the pool to delete the object from
@param oid the name of the object to delete
@returns 0 on success, negative error code on failure

CEPH_RADOS_API int rados_remove(rados_ioctx_t io, const char *oid);
]]
function _M.rados_remove(ioctx,oid)
	 local oid1 = ffi.cast("const char *",oid)
	 ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ", "oid is ",oid)
         local err = librados.rados_remove(ioctx[0])
	 if err < 0 then
	       do 
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " rados remove err code is ", tonumber(err))
	        --ioctxdestory
                --shutdown cluster
	       return err 
               end
         else 
	      return 0

	 end
end
--[[
/**
 * Write *len* bytes from *buf* into the *oid* object. The value of
 * *len* must be <= UINT_MAX/2.
 *
 * The object is filled with the provided data. If the object exists,
 * it is atomically truncated and then written.
 *
 * @param io the io context in which the write will occur
 * @param oid name of the object
 * @param buf data to write
 * @param len length of the data, in bytes
 * @returns 0 on success, negative error code on failure
 */
CEPH_RADOS_API int rados_write_full(rados_ioctx_t io, const char *oid,
                                    const char *buf, size_t len);
]]

function _M.rados_write_full(ioctx,oid,buf,len)
         local err = ffi.new("int")
         local len1 = ffi.new("size_t",len)
         err = librados.rados_write_full(ioctx[0],oid,buf,len)

         if err < 0 then
         do
           print "write object fail"
         --ioctxdestory
         --shutdown cluster
           return err
         end
         else
           return 0    
         end
end
--[[
all sync op
--rados_remove,rados_clone_range, rados_append, rados_read, rados_trunc,
--rados_getxattr rados_setxattr rados_rmxattr rados_getxattrs rados_getxattrs_next 
--rados_getxattrs_end rados_omap_get_next rados_stat(222) rados_tmap_update
--obj class method
/**
 * Execute an OSD class method on an object
 *
 * The OSD has a plugin mechanism for performing complicated
 * operations on an object atomically. These plugins are called
 * classes. This function allows librados users to call the custom
 * methods. The input and output formats are defined by the class.
 * Classes in ceph.git can be found in src/cls subdirectories
 *
 * @param io the context in which to call the method
 * @param oid the object to call the method on
 * @param cls the name of the class
 * @param method the name of the method
 * @param in_buf where to find input
 * @param in_len length of in_buf in bytes
 * @param buf where to store output
 * @param out_len length of buf in bytes
 * @returns the length of the output, or
 * -ERANGE if out_buf does not have enough space to store it (For methods that return data). For
 * methods that don't return data, the return value is
 * method-specific.
 */
CEPH_RADOS_API int rados_exec(rados_ioctx_t io, const char *oid,
                              const char *cls, const char *method,
                              const char *in_buf, size_t in_len, char *buf,
                              size_t out_len);


]]
--[[
ASYCHRONOUS IO
ana io
rados_aio_write rados_aio_append rados_aio_write_full dos_aio_remove rados_aio_read rados_aio_flush 
 rados_aio_stat rados_aio_flush_async rados_aio_cancel

]]
--[[
/**
 * Constructs a completion to use with asynchronous operations
 *
 * The complete and safe callbacks correspond to operations being
 * acked and committed, respectively. The callbacks are called in
 * order of receipt, so the safe callback may be triggered before the
 * complete callback, and vice versa. This is affected by journalling
 * on the OSDs.
 *
 * TODO: more complete documentation of this elsewhere (in the RADOS docs?)
 *
 * @note Read operations only get a complete callback.
 * @note BUG: this should check for ENOMEM instead of throwing an exception
 *
 * @param cb_arg application-defined data passed to the callback functions
 * @param cb_complete the function to be called when the operation is
 * in memory on all relpicas
 * @param cb_safe the function to be called when the operation is on
 * stable storage on all replicas
 * @param pc where to store the completion
 * @returns 0
 */
CEPH_RADOS_API int rados_aio_create_completion(void *cb_arg,
                                               rados_callback_t cb_complete,
                                               rados_callback_t cb_safe,
                                               rados_completion_t *pc);
]]
--[[
/**
 * Write data to an object asynchronously
 *
 * Queues the write and returns. The return value of the completion
 * will be 0 on success, negative error code on failure.
 *
 * @param io the context in which the write will occur
 * @param oid name of the object
 * @param completion what to do when the write is safe and complete
 * @param buf data to write
 * @param len length of the data, in bytes
 * @param off byte offset in the object to begin writing at
 * @returns 0 on success, -EROFS if the io context specifies a snap_seq
 * other than LIBRADOS_SNAP_HEAD
 */
CEPH_RADOS_API int rados_aio_write(rados_ioctx_t io, const char *oid,
                                   rados_completion_t completion,
                                   const char *buf, size_t len, uint64_t off);
]]
function _M.rados_aio_write(ioctx,oid,comp,buf,len,off)
         local err = ffi.new("int")
         local len1 = ffi.new("size_t",len)
	 local oid1 = ffi.cast("const char *",oid)
--	 local buf1 = ffi.cast("const char *",buf)
	 err = librados.rados_aio_write(ioctx[0],oid1,comp[0],buf,len1,off)
	 if err == 0 then
		 do
		  return err --err code is ?
		 end
	 else 
		  return err
         end

   --cb_arg from cb_complete,cb_safe from lua,pcomp there. 
   --rados_aio_create_completion(cb_arg,  cb_complete, cb_safe, pcomp)
   --librados.rados_aio_write(ioctx,oid,comp,buf,len,off)
           
end
--[[
/**
 * Asychronously read data from an object
 *
 * The io context determines the snapshot to read from, if any was set
 * by rados_ioctx_snap_set_read().
 *
 * The return value of the completion will be number of bytes read on
 * success, negative error code on failure.
 *
 * @note only the 'complete' callback of the completion will be called.
 *
 * @param io the context in which to perform the read
 * @param oid the name of the object to read from
 * @param completion what to do when the read is complete
 * @param buf where to store the results
 * @param len the number of bytes to read
 * @param off the offset to start reading from in the object
 * @returns 0 on success, negative error code on failure
 */
CEPH_RADOS_API int rados_aio_read(rados_ioctx_t io, const char *oid,
		                  rados_completion_t completion,

		                  char *buf, size_t len, uint64_t off);
]]

function _M.rados_aio_read(ioctx,oid,comp,buf,len,off)
         local err = ffi.new("int")
         local len1 = ffi.new("size_t",len)
	 local oid1 = ffi.cast("const char *",oid)
--	 local buf1 = ffi.cast("const char *",buf)
--	 local buf1 = ffi.new("char[?]",len1) --malloc alanative?
	 err = librados.rados_aio_read(ioctx[0],oid1,comp[0],buf,len1,off)
	 if err < 0 then
		 do
		  return err --err code is ?
		 end
	 else 
	 --   local luabuf = ffi.string(buf,len1)
	 --   ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ", "luabuf is ",luabuf)
	     --return 0
	     return 0

         end
end

--[[
 @returns 0 on success, -EROFS if the io context specifies a snap_seq
 * other than LIBRADOS_SNAP_HEAD
 */
CEPH_RADOS_API int rados_aio_write_full(rados_ioctx_t io, const char *oid,
                                        rados_completion_t completion,
                                        const char *buf, size_t len);
 ]]
function _M.rados_aio_write_full(ioctx,oid,pcomp,buf,len)
         local err = ffi.new("int")
         local len1 = ffi.new("size_t",len)
	 err = librados.rados_aio_write_full(ioctx[0],oid,pcomp[0],buf,len1)
	 if err == 0 then
		 do
		  return err --err code is ?
		 end
	 else 
		  return err
         end
end
function _M.rados_aio_create_completion(cb_arg, cb_complete, cb_safe)
	local pc = ffi.new("rados_completion_t [1]")
        local cbarg,vbarg


        if cb_arg == nil  then
        librados.rados_aio_create_completion(cb_arg, cb_complete, cb_safe,pc)

        else 
           cbarg = ffi.new("int [1]")

	   cbarg[0] = cb_arg
           print ("cb_arg is",cb_arg,"cbarg[0] is",cbarg[0])

--	vbarg = ffi.cast("void *",cbarg)


--	local cbarg = ffi.new("int [1]")
--	cbarg[0] = cb_arg
--	vbarg = ffi.cast("void *",cbarg)

--        local voidtype = ffi.typeof("void *")
--	local vbarg = voidtype(cbarg)
        vbarg = ffi.cast("void *",cbarg)
        print("vbarg is",vbarg)
	--stat_type(cb_arg[0])
--	cbarg[0] = cb_arg
	-- automic cast ?      
	 librados.rados_aio_create_completion(vbarg, ffi.cast("rados_callback_t ",cb_complete),ffi.cast("rados_callback_t ",cb_safe),pc) 
        end
 
         print ("end is librados.rados_aio_create_completion")
	return pc

end

function _M.rados_wait_for_complete(pcomp) 
         local err = ffi.new("int")
 	 err = librados.rados_aio_wait_for_complete(pcomp[0])
         print "in function wait for complete"
	 return err
end --in mem
function _M.rados_wait_for_safe(pcomp) 
         local err = ffi.new("int")
	 err = librados.rados_aio_wait_for_safe(pcomp[0])
         print "in function wait for safe"
	 return err
end  -- in disk
function _M.rados_aio_flush(ioctx)
         local err = ffi.new("int")
	 err = librados.rados_aio_flush(ioctx);
	 if err < 0 then
		 do
			 return err --err code?
	         end
         else 
		 return 0
	 end
end

function _M.rados_aio_release(comp)
         --local err = ffi.new("int")
	 librados.rados_aio_release(comp[0])
         print " release comp resource "
end
             
function _M.rados_shutdown(clusterh)
         librados.rados_shutdown(clusterh[0])
end

return _M
