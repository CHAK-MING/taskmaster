#include "dagforge/executor/executor.hpp"
#include "dagforge/executor/executor_state.hpp"

#include "dagforge/util/log.hpp"
#include "dagforge/util/json.hpp"

#include <atomic>
#include <charconv>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <format>
#include <memory>
#include <experimental/scope>
#include <cstdio>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

extern "C" {
#include "../../../third_party/lua-5.4.8/src/lauxlib.h"
#include "../../../third_party/lua-5.4.8/src/lua.h"
#include "../../../third_party/lua-5.4.8/src/lualib.h"
}

namespace dagforge {

namespace {

struct LuaAllocState {
  std::size_t bytes_in_use{0};
  std::size_t max_bytes{8ULL * 1024ULL * 1024ULL};
};

struct LuaHookState {
  std::uint64_t remaining_instructions{100'000};
  std::chrono::steady_clock::time_point deadline{};
  std::atomic_bool *cancel_requested{nullptr};
};

inline constexpr int kLuaHookGranularity = 1000;
inline const char *kLuaHookRegistryKey = "dagforge.lua.hook_state";
inline constexpr auto kHeartbeatInterval = std::chrono::seconds(1);
inline constexpr auto kLuaSleepPollInterval = std::chrono::milliseconds(10);

struct ActiveLuaTask {
  std::shared_ptr<std::atomic_bool> cancel_requested;
  std::shared_ptr<std::jthread> worker;
};

using LuaShardState = ExecutorShardState<ActiveLuaTask>;

auto lua_allocator(void *ud, void *ptr, size_t osize, size_t nsize) -> void * {
  auto *state = static_cast<LuaAllocState *>(ud);
  if (state == nullptr) {
    return nullptr;
  }

  const std::size_t old_size = ptr != nullptr ? osize : 0;
  if (nsize == 0) {
    if (ptr != nullptr) {
      if (state->bytes_in_use >= old_size) {
        state->bytes_in_use -= old_size;
      } else {
        log::warn(
            "Lua allocator accounting underflow detected: bytes_in_use={} "
            "old_size={}",
            state->bytes_in_use, old_size);
        state->bytes_in_use = 0;
      }
    }
    std::free(ptr);
    return nullptr;
  }

  const auto tentative_bytes =
      state->bytes_in_use >= old_size
          ? state->bytes_in_use - old_size + nsize
          : nsize;
  if (tentative_bytes > state->max_bytes) {
    return nullptr;
  }

  void *next = std::realloc(ptr, nsize);
  if (next != nullptr) {
    state->bytes_in_use = tentative_bytes;
  }
  return next;
}

auto load_hook_state(lua_State *L) -> LuaHookState * {
  lua_pushlightuserdata(L, const_cast<char *>(kLuaHookRegistryKey));
  lua_gettable(L, LUA_REGISTRYINDEX);
  auto *state = static_cast<LuaHookState *>(lua_touserdata(L, -1));
  lua_pop(L, 1);
  return state;
}

int lua_traceback_handler(lua_State *L) {
  size_t size = 0;
  const char *message = lua_tolstring(L, 1, &size);
  if (message == nullptr) {
    if (lua_isnoneornil(L, 1)) {
      lua_pushliteral(L, "unknown lua error");
    } else {
      lua_pushvalue(L, 1);
    }
    return 1;
  }

  luaL_Buffer buffer;
  luaL_buffinit(L, &buffer);
  luaL_addlstring(&buffer, message, size);
  luaL_addstring(&buffer, "\nstack traceback:");

  lua_Debug ar;
  for (int level = 1; lua_getstack(L, level, &ar) != 0; ++level) {
    lua_getinfo(L, "Sln", &ar);
    luaL_addstring(&buffer, "\n\t");
    luaL_addstring(&buffer, ar.short_src[0] != '\0' ? ar.short_src : "?");
    if (ar.currentline > 0) {
      char line[32];
      std::snprintf(line, sizeof(line), ":%d", ar.currentline);
      luaL_addstring(&buffer, line);
    }
    if (ar.name != nullptr && *ar.name != '\0') {
      luaL_addstring(&buffer, " in function '");
      luaL_addstring(&buffer, ar.name);
      luaL_addstring(&buffer, "'");
    }
  }

  luaL_pushresult(&buffer);
  return 1;
}

void instruction_hook(lua_State *L, lua_Debug * /*debug*/) {
  auto *state = load_hook_state(L);
  if (state == nullptr) {
    return;
  }

  if (std::chrono::steady_clock::now() > state->deadline) {
    luaL_error(L, "Lua execution timeout");
  }

  if (state->cancel_requested != nullptr &&
      state->cancel_requested->load(std::memory_order_acquire)) {
    luaL_error(L, "Lua execution cancelled");
  }

  if (state->remaining_instructions <= static_cast<std::uint64_t>(kLuaHookGranularity)) {
    luaL_error(L, "Lua instruction limit exceeded");
  }
  state->remaining_instructions -= static_cast<std::uint64_t>(kLuaHookGranularity);
}

auto load_script_source(const LuaExecutorConfig &config,
                        std::string_view working_dir) -> Result<std::string> {
  if (config.script.empty() == config.script_file.empty()) {
    return fail(Error::InvalidArgument,
                "Lua executor requires exactly one of script or script_file");
  }
  if (!config.script.empty()) {
    return ok(config.script);
  }

  auto path = std::filesystem::path(config.script_file);
  if (path.is_relative() && !working_dir.empty()) {
    path = std::filesystem::path(working_dir) / path;
  }

  std::ifstream input(path, std::ios::in | std::ios::binary);
  if (!input.is_open()) {
    return fail(Error::NotFound,
                std::format("Failed to open lua script file '{}'",
                            path.string()));
  }

  std::string content{std::istreambuf_iterator<char>(input),
                      std::istreambuf_iterator<char>()};
  return ok(std::move(content));
}

struct LuaExecutionContext {
  const ExecutorRequest *request{nullptr};
  ExecutionSink *sink{nullptr};
  ExecutorResult *result{nullptr};
  ExecutorRequest::LuaRuntimeContext *lua_context{nullptr};
};

inline const char *kLuaExecContextRegistryKey = "dagforge.lua.exec_context";

auto load_execution_context(lua_State *L) -> LuaExecutionContext * {
  lua_pushlightuserdata(L, const_cast<char *>(kLuaExecContextRegistryKey));
  lua_gettable(L, LUA_REGISTRYINDEX);
  auto *state = static_cast<LuaExecutionContext *>(lua_touserdata(L, -1));
  lua_pop(L, 1);
  return state;
}

auto lua_to_json_value(lua_State *L, int index, int depth = 0)
    -> Result<JsonValue>;

void push_json_value(lua_State *L, const JsonValue &value) {
  if (value.is_null()) {
    lua_pushnil(L);
    return;
  }
  if (value.is_string()) {
    const auto &text = value.as<std::string>();
    lua_pushlstring(L, text.data(), text.size());
    return;
  }
  if (value.is_boolean()) {
    lua_pushboolean(L, value.as<bool>() ? 1 : 0);
    return;
  }
  if (value.is_number()) {
    lua_pushnumber(L, static_cast<lua_Number>(value.as<double>()));
    return;
  }
  if (value.is_array()) {
    lua_newtable(L);
    int lua_index = 1;
    for (const auto &item : value.get_array()) {
      push_json_value(L, item);
      lua_rawseti(L, -2, lua_index++);
    }
    return;
  }
  if (value.is_object()) {
    lua_newtable(L);
    for (const auto &[key, item] : value.get_object()) {
      push_json_value(L, item);
      lua_setfield(L, -2, key.c_str());
    }
    return;
  }
  lua_pushnil(L);
}

auto table_is_dense_array(lua_State *L, int index, std::size_t &max_index)
    -> bool {
  const int abs_index = index < 0 ? lua_gettop(L) + index + 1 : index;
  max_index = 0;
  std::vector<bool> seen;
  lua_pushnil(L);
  while (lua_next(L, abs_index) != 0) {
    if (lua_type(L, -2) != LUA_TNUMBER) {
      lua_pop(L, 2);
      return false;
    }
    const lua_Number raw = lua_tonumber(L, -2);
    if (!std::isfinite(raw) || raw < 1.0 || std::floor(raw) != raw) {
      lua_pop(L, 2);
      return false;
    }
    const auto key = static_cast<std::size_t>(raw);
    if (key > max_index) {
      max_index = key;
      seen.resize(max_index, false);
    }
    seen[key - 1] = true;
    lua_pop(L, 1);
  }
  return std::ranges::all_of(seen, [](bool present) { return present; });
}

auto lua_to_json_value(lua_State *L, int index, int depth) -> Result<JsonValue> {
  if (depth > 64) {
    return fail(Error::InvalidArgument, "Lua value nesting too deep");
  }

  switch (lua_type(L, index)) {
  case LUA_TNIL:
    return ok(JsonValue(nullptr));
  case LUA_TBOOLEAN:
    return ok(JsonValue(lua_toboolean(L, index) != 0));
  case LUA_TNUMBER: {
    const auto raw = static_cast<double>(lua_tonumber(L, index));
    if (!std::isfinite(raw)) {
      return fail(Error::InvalidArgument,
                  "Lua numbers must be finite for JSON encoding");
    }
    return ok(JsonValue(raw));
  }
  case LUA_TSTRING: {
    size_t size = 0;
    const char *text = lua_tolstring(L, index, &size);
    return ok(JsonValue(std::string(text, size)));
  }
  case LUA_TTABLE: {
    std::size_t max_index = 0;
    if (table_is_dense_array(L, index, max_index)) {
      JsonValue array = JsonValue::array_t{};
      auto &items = array.get_array();
      items.reserve(max_index);
      const int abs_index = index < 0 ? lua_gettop(L) + index + 1 : index;
      for (std::size_t i = 1; i <= max_index; ++i) {
        lua_rawgeti(L, abs_index, static_cast<int>(i));
        auto item = lua_to_json_value(L, -1, depth + 1);
        lua_pop(L, 1);
        if (!item) {
          return fail(item.error());
        }
        items.emplace_back(std::move(*item));
      }
      return ok(std::move(array));
    }

    JsonValue object = JsonValue::object_t{};
    auto &members = object.get_object();
    const int abs_index = index < 0 ? lua_gettop(L) + index + 1 : index;
    lua_pushnil(L);
    while (lua_next(L, abs_index) != 0) {
      if (lua_type(L, -2) != LUA_TSTRING) {
        lua_pop(L, 2);
        return fail(Error::InvalidArgument,
                    "Lua tables must use string keys or dense integer keys");
      }
      size_t key_size = 0;
      const char *key = lua_tolstring(L, -2, &key_size);
      auto item = lua_to_json_value(L, -1, depth + 1);
      lua_pop(L, 1);
      if (!item) {
        return fail(item.error());
      }
      members.emplace(std::string(key, key_size), std::move(*item));
    }
    return ok(std::move(object));
  }
  default:
    return fail(Error::InvalidArgument,
                std::format("Unsupported lua value type '{}'",
                            lua_typename(L, lua_type(L, index))));
  }
}

auto make_stdout_from_return(lua_State *L, int index) -> Result<std::string> {
  switch (lua_type(L, index)) {
  case LUA_TTABLE: {
    auto json = lua_to_json_value(L, index);
    if (!json) {
      return fail(json.error());
    }
    return ok(dump_json(*json));
  }
  default:
    break;
  }
  switch (lua_type(L, index)) {
  case LUA_TNIL:
    return ok(std::string{});
  case LUA_TSTRING:
  case LUA_TNUMBER: {
    size_t size = 0;
    const char *text = lua_tolstring(L, index, &size);
    return ok(std::string(text, size));
  }
  case LUA_TBOOLEAN:
    return ok(lua_toboolean(L, index) != 0 ? std::string{"true"}
                                           : std::string{"false"});
  default:
    return fail(Error::InvalidArgument,
                std::format("Unsupported lua return type '{}'",
                            lua_typename(L, lua_type(L, index))));
  }
}

int lua_dagforge_log(lua_State *L) {
  auto *exec = load_execution_context(L);
  if (exec == nullptr || exec->result == nullptr) {
    return luaL_error(L, "Lua execution context unavailable");
  }
  size_t size = 0;
  const char *message = luaL_checklstring(L, 1, &size);
  if (exec->lua_context != nullptr && exec->lua_context->on_log) {
    exec->lua_context->on_log(std::string_view(message, size));
  } else if (exec->sink != nullptr && exec->sink->on_stdout) {
    exec->sink->on_stdout(exec->request->instance_id,
                          std::string_view(message, size));
  }
  exec->result->stdout_streamed = true;
  return 0;
}

int lua_dagforge_sleep(lua_State *L) {
  auto *hook_state = load_hook_state(L);
  if (hook_state == nullptr) {
    return luaL_error(L, "Lua hook state unavailable");
  }

  const lua_Number raw_ms = luaL_checknumber(L, 1);
  if (!std::isfinite(raw_ms) || raw_ms < 0.0) {
    return luaL_error(L, "sleep duration must be a non-negative finite number");
  }

  auto remaining = std::chrono::milliseconds(
      static_cast<std::int64_t>(std::llround(raw_ms)));
  while (remaining > std::chrono::milliseconds::zero()) {
    if (std::chrono::steady_clock::now() > hook_state->deadline) {
      return luaL_error(L, "Lua execution timeout");
    }
    if (hook_state->cancel_requested != nullptr &&
        hook_state->cancel_requested->load(std::memory_order_acquire)) {
      return luaL_error(L, "Lua execution cancelled");
    }

    const auto slice = std::min(remaining, kLuaSleepPollInterval);
    std::this_thread::sleep_for(slice);
    remaining -= slice;
  }

  return 0;
}

int lua_dagforge_json_decode(lua_State *L) {
  size_t size = 0;
  const char *text = luaL_checklstring(L, 1, &size);
  auto parsed = parse_json(std::string_view(text, size));
  if (!parsed) {
    return luaL_error(L, "%s", parsed.error().message().c_str());
  }
  push_json_value(L, *parsed);
  return 1;
}

int lua_dagforge_json_encode(lua_State *L) {
  auto encoded = lua_to_json_value(L, 1);
  if (!encoded) {
    return luaL_error(L, "%s", encoded.error().message().c_str());
  }
  const auto text = dump_json(*encoded);
  lua_pushlstring(L, text.data(), text.size());
  return 1;
}

void install_dagforge_api(lua_State *L, const ExecutorRequest &req,
                          LuaExecutionContext &exec_ctx) {
  lua_pushlightuserdata(L, const_cast<char *>(kLuaExecContextRegistryKey));
  lua_pushlightuserdata(L, &exec_ctx);
  lua_settable(L, LUA_REGISTRYINDEX);

  lua_newtable(L);
  lua_pushcfunction(L, &lua_dagforge_log);
  lua_setfield(L, -2, "log");
  lua_pushcfunction(L, &lua_dagforge_sleep);
  lua_setfield(L, -2, "sleep");
  lua_pushcfunction(L, &lua_dagforge_json_decode);
  lua_setfield(L, -2, "json_decode");
  lua_pushcfunction(L, &lua_dagforge_json_encode);
  lua_setfield(L, -2, "json_encode");

  if (req.lua_context != nullptr) {
    lua_pushlstring(L, req.lua_context->task_id.value().data(),
                    req.lua_context->task_id.value().size());
    lua_setfield(L, -2, "task_id");
    lua_pushlstring(L, req.lua_context->dag_run_id.value().data(),
                    req.lua_context->dag_run_id.value().size());
    lua_setfield(L, -2, "run_id");
    lua_pushlstring(L, req.lua_context->dag_id.value().data(),
                    req.lua_context->dag_id.value().size());
    lua_setfield(L, -2, "dag_id");
    lua_pushlstring(L, req.lua_context->execution_date.data(),
                    req.lua_context->execution_date.size());
    lua_setfield(L, -2, "execution_date");

    lua_newtable(L);
    for (const auto &[key, value] : req.lua_context->conf_values) {
      if (auto parsed = parse_json(value)) {
        push_json_value(L, *parsed);
      } else {
        lua_pushlstring(L, value.data(), value.size());
      }
      lua_setfield(L, -2, key.c_str());
    }
    lua_setfield(L, -2, "conf");
  } else {
    lua_pushstring(L, "");
    lua_setfield(L, -2, "task_id");
    lua_pushstring(L, "");
    lua_setfield(L, -2, "run_id");
    lua_pushstring(L, "");
    lua_setfield(L, -2, "dag_id");
    lua_pushstring(L, "");
    lua_setfield(L, -2, "execution_date");
    lua_newtable(L);
    lua_setfield(L, -2, "conf");
  }
  lua_setglobal(L, "dagforge");
}

auto run_lua_chunk(ExecutorRequest req, ExecutionSink &sink,
                   const std::shared_ptr<std::atomic_bool> &cancel_requested)
    -> ExecutorResult {
  auto *resource = req.resource();
  ExecutorResult result = make_executor_result(resource);

  const auto *config = req.config.as<LuaExecutorConfig>();
  if (config == nullptr) {
    result.exit_code = 1;
    result.error = pmr::string(
        std::format("Invalid executor config for lua executor on instance '{}'",
                    req.instance_id.value()),
        resource);
    return result;
  }

  auto script_source = load_script_source(*config, req.working_dir);
  if (!script_source) {
    result.exit_code = 1;
    result.error = pmr::string(script_source.error().message(), resource);
    return result;
  }

  LuaAllocState alloc_state{.bytes_in_use = 0,
                            .max_bytes = static_cast<std::size_t>(
                                config->max_memory_bytes)};
  lua_State *L = lua_newstate(lua_allocator, &alloc_state);
  if (L == nullptr) {
    result.exit_code = 1;
    result.error = pmr::string("Failed to create lua state", resource);
    return result;
  }
  const auto close_state = std::unique_ptr<lua_State, decltype(&lua_close)>(
      L, &lua_close);
  LuaExecutionContext exec_ctx{
      .request = &req,
      .sink = &sink,
      .result = &result,
      .lua_context = req.lua_context.get(),
  };

  luaL_openlibs(L);
  for (const char *name :
       {"io", "os", "package", "debug", "coroutine", "require", "module",
        "dofile", "loadfile", "loadstring", "load"}) {
    lua_pushnil(L);
    lua_setglobal(L, name);
  }
  install_dagforge_api(L, req, exec_ctx);

  LuaHookState hook_state{
      .remaining_instructions = config->max_instructions,
      .deadline = std::chrono::steady_clock::now() + req.execution_timeout,
      .cancel_requested = cancel_requested.get(),
  };
  lua_pushlightuserdata(L, const_cast<char *>(kLuaHookRegistryKey));
  lua_pushlightuserdata(L, &hook_state);
  lua_settable(L, LUA_REGISTRYINDEX);
  lua_sethook(L, &instruction_hook, LUA_MASKCOUNT, kLuaHookGranularity);

  if (sink.on_state) {
    sink.on_state(req.instance_id, "started");
  }

  if (luaL_loadbuffer(L, script_source->data(), script_source->size(),
                      config->script_file.empty() ? "task.lua"
                                                  : config->script_file.c_str()) != 0) {
    result.exit_code = 1;
    result.error = pmr::string(lua_tostring(L, -1), resource);
    return result;
  }

  lua_pushcfunction(L, &lua_traceback_handler);
  lua_insert(L, -2);
  const int errfunc = lua_gettop(L) - 1;
  if (lua_pcall(L, 0, LUA_MULTRET, errfunc) != 0) {
    result.exit_code = 1;
    result.error = pmr::string(lua_tostring(L, -1), resource);
    return result;
  }

  lua_remove(L, errfunc);

  const int result_count = lua_gettop(L);
  if (result_count > 0) {
    auto stdout_text = make_stdout_from_return(L, 1);
    if (!stdout_text) {
      result.exit_code = 1;
      result.error = pmr::string(stdout_text.error().message(), resource);
      return result;
    }
    result.stdout_output = pmr::string(*stdout_text, resource);
  }

  return result;
}

auto emit_heartbeat(
    const std::shared_ptr<ExecutorHeartbeatCallback> &heartbeat_callback,
    const InstanceId &instance_id) -> void {
  if (heartbeat_callback && *heartbeat_callback) {
    (*heartbeat_callback)(instance_id);
  }
}

auto run_executor_heartbeat(
    std::shared_ptr<ExecutorHeartbeatCallback> heartbeat_callback,
    std::shared_ptr<std::atomic_bool> stop, InstanceId instance_id)
    -> spawn_task {
  if (!heartbeat_callback || !*heartbeat_callback) {
    co_return;
  }

  while (!stop->load(std::memory_order_acquire)) {
    try {
      co_await async_sleep_on_timing_wheel(kHeartbeatInterval);
    } catch (const std::exception &) {
      co_return;
    }
    if (stop->load(std::memory_order_acquire)) {
      co_return;
    }
    (*heartbeat_callback)(instance_id);
  }
}

class LuaExecutor final : public IExecutor {
public:
  explicit LuaExecutor(Runtime &runtime)
      : runtime_{&runtime}, shard_states_(runtime.shard_count()) {}

  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {
    const auto *config = req.config.as<LuaExecutorConfig>();
    if (config == nullptr) {
      return fail(Error::InvalidArgument,
                  std::format("Invalid executor config for lua executor on instance '{}'",
                              req.instance_id.value()));
    }

    auto sid = runtime_->is_current_shard() ? runtime_->current_shard() : 0;
    auto resource_owner = req.memory_resource;
    auto shared_sink = std::make_shared<ExecutionSink>(std::move(sink));
    auto cancel_requested = std::make_shared<std::atomic_bool>(false);
    auto heartbeat_stop = std::make_shared<std::atomic_bool>(false);
    std::shared_ptr<ExecutorHeartbeatCallback> heartbeat_callback;
    if (shared_sink->on_heartbeat) {
      heartbeat_callback = std::make_shared<ExecutorHeartbeatCallback>(
          std::move(shared_sink->on_heartbeat));
    }

    ExecutionSink marshaled_sink;
    if (shared_sink->on_state) {
      marshaled_sink.on_state =
          [runtime = runtime_, sid, shared_sink](const InstanceId &instance_id,
                                                 std::string_view message) {
            runtime->post_to(sid, [shared_sink, instance_id = instance_id.clone(),
                                   message = std::string(message)]() mutable {
              if (shared_sink->on_state) {
                shared_sink->on_state(instance_id, message);
              }
            });
          };
    }
    if (shared_sink->on_stdout) {
      marshaled_sink.on_stdout =
          [runtime = runtime_, sid, shared_sink](const InstanceId &instance_id,
                                                 std::string_view data) {
            runtime->post_to(sid, [shared_sink, instance_id = instance_id.clone(),
                                   data = std::string(data)]() mutable {
              if (shared_sink->on_stdout) {
                shared_sink->on_stdout(instance_id, data);
              }
            });
          };
    }
    if (shared_sink->on_stderr) {
      marshaled_sink.on_stderr =
          [runtime = runtime_, sid, shared_sink](const InstanceId &instance_id,
                                                 std::string_view data) {
            runtime->post_to(sid, [shared_sink, instance_id = instance_id.clone(),
                                   data = std::string(data)]() mutable {
              if (shared_sink->on_stderr) {
                shared_sink->on_stderr(instance_id, data);
              }
            });
          };
    }

    emit_heartbeat(heartbeat_callback, req.instance_id);
    if (heartbeat_callback && *heartbeat_callback) {
      runtime_->spawn(run_executor_heartbeat(heartbeat_callback, heartbeat_stop,
                                             req.instance_id.clone()));
    }

    auto instance_id = req.instance_id.clone();
    auto *state = &shard_states_[sid];
    auto worker = std::make_shared<std::jthread>(
        [runtime = runtime_, sid, req = std::move(req),
         sink = std::move(marshaled_sink), shared_sink, cancel_requested,
         heartbeat_stop, instance_id = instance_id.clone(), state,
         resource_owner]() mutable {
          const auto stop_heartbeat = std::experimental::scope_exit(
              [heartbeat_stop] {
                heartbeat_stop->store(true, std::memory_order_release);
              });

          auto run_req = std::move(req);
          run_req.memory_resource = resource_owner;
          auto result =
              run_lua_chunk(std::move(run_req), sink, cancel_requested);
          runtime->post_to(
              sid, [state, shared_sink, instance_id = instance_id.clone(),
                    result = std::move(result)]() mutable {
                state->unregister_active(instance_id);
                if (shared_sink->on_complete) {
                  shared_sink->on_complete(instance_id.clone(),
                                           std::move(result));
                }
              });
        });
    state->register_active(
        instance_id,
        ActiveLuaTask{.cancel_requested = cancel_requested, .worker = worker});
    return ok();
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    cancel_on_all_shards(*runtime_, shard_states_, instance_id,
                         [](LuaShardState &state, const InstanceId &id) {
                           auto it = state.find_active_mut(id);
                           if (it == state.active_end() ||
                               !it->second.cancel_requested) {
                             return;
                           }
                           it->second.cancel_requested->store(
                               true, std::memory_order_release);
                           log::debug("LuaExecutor cancel: instance_id={}", id);
                         });
  }

private:
  Runtime *runtime_;
  std::vector<LuaShardState> shard_states_;
};

} // namespace

auto create_lua_executor(Runtime &rt) -> std::unique_ptr<IExecutor> {
  return std::make_unique<LuaExecutor>(rt);
}

} // namespace dagforge
