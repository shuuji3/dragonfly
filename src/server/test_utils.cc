// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/test_utils.h"

extern "C" {
#include "redis/zmalloc.h"
}

#include <absl/strings/match.h>
#include <absl/strings/str_split.h>
#include <mimalloc.h>

#include "base/flags.h"
#include "base/logging.h"
#include "base/stl_util.h"
#include "facade/dragonfly_connection.h"
#include "util/uring/uring_pool.h"

using namespace std;

ABSL_DECLARE_FLAG(string, dbfilename);

namespace dfly {
using MP = MemcacheParser;
using namespace util;
using namespace testing;

static vector<string> SplitLines(const std::string& src) {
  vector<string> res = absl::StrSplit(src, "\r\n");
  if (res.back().empty())
    res.pop_back();
  for (auto& v : res) {
    absl::StripAsciiWhitespace(&v);
  }
  return res;
}

TestConnection::TestConnection(Protocol protocol)
    : facade::Connection(protocol, nullptr, nullptr, nullptr) {
}

void TestConnection::SendMsgVecAsync(const PubMessage& pmsg, util::fibers_ext::BlockingCounter bc) {
  backing_str_.emplace_back(new string(pmsg.channel));
  PubMessage dest;
  dest.channel = *backing_str_.back();

  backing_str_.emplace_back(new string(pmsg.message));
  dest.message = *backing_str_.back();

  if (!pmsg.pattern.empty()) {
    backing_str_.emplace_back(new string(pmsg.pattern));
    dest.pattern = *backing_str_.back();
  }
  messages.push_back(dest);

  bc.Dec();
}

class BaseFamilyTest::TestConnWrapper {
 public:
  TestConnWrapper(Protocol proto);
  ~TestConnWrapper();

  CmdArgVec Args(ArgSlice list);

  RespVec ParseResponse();

  // returns: type(pmessage), pattern, channel, message.
  facade::Connection::PubMessage GetPubMessage(size_t index) const;

  ConnectionContext* cmd_cntx() {
    return &cmd_cntx_;
  }

  StringVec SplitLines() const {
    return dfly::SplitLines(sink_.str());
  }

  void ClearSink() {
    sink_.Clear();
  }

  TestConnection* conn() {
    return dummy_conn_.get();
  }

 private:
  ::io::StringSink sink_;  // holds the response blob

  std::unique_ptr<TestConnection> dummy_conn_;

  ConnectionContext cmd_cntx_;
  std::vector<std::unique_ptr<std::string>> tmp_str_vec_;

  std::unique_ptr<RedisParser> parser_;
};

BaseFamilyTest::TestConnWrapper::TestConnWrapper(Protocol proto)
    : dummy_conn_(new TestConnection(proto)), cmd_cntx_(&sink_, dummy_conn_.get()) {
}

BaseFamilyTest::TestConnWrapper::~TestConnWrapper() {
}

BaseFamilyTest::BaseFamilyTest() {
}

BaseFamilyTest::~BaseFamilyTest() {
  for (auto* v : resp_vec_)
    delete v;
}

void BaseFamilyTest::SetUpTestSuite() {
  absl::SetFlag(&FLAGS_dbfilename, "");
  init_zmalloc_threadlocal(mi_heap_get_backing());
}

void BaseFamilyTest::SetUp() {
  pp_.reset(new uring::UringPool(16, num_threads_));
  pp_->Run();
  service_.reset(new Service{pp_.get()});

  Service::InitOpts opts;
  opts.disable_time_update = true;
  service_->Init(nullptr, nullptr, opts);

  expire_now_ = absl::GetCurrentTimeNanos() / 1000000;
  auto cb = [&](EngineShard* s) {
    s->db_slice().UpdateExpireBase(expire_now_ - 1000, 0);
    s->db_slice().UpdateExpireClock(expire_now_);
  };
  shard_set->RunBriefInParallel(cb);

  const TestInfo* const test_info = UnitTest::GetInstance()->current_test_info();
  LOG(INFO) << "Starting " << test_info->name();
}

void BaseFamilyTest::TearDown() {
  service_->Shutdown();
  service_.reset();
  pp_->Stop();

  const TestInfo* const test_info = UnitTest::GetInstance()->current_test_info();
  LOG(INFO) << "Finishing " << test_info->name();
}

// ts is ms
void BaseFamilyTest::UpdateTime(uint64_t ms) {
  auto cb = [ms](EngineShard* s) { s->db_slice().UpdateExpireClock(ms); };
  shard_set->RunBriefInParallel(cb);
}

RespExpr BaseFamilyTest::Run(ArgSlice list) {
  if (!ProactorBase::IsProactorThread()) {
    return pp_->at(0)->Await([&] { return this->Run(list); });
  }

  return Run(GetId(), list);
}

RespExpr BaseFamilyTest::Run(std::string_view id, ArgSlice slice) {
  TestConnWrapper* conn_wrapper = AddFindConn(Protocol::REDIS, id);

  CmdArgVec args = conn_wrapper->Args(slice);

  auto* context = conn_wrapper->cmd_cntx();

  DCHECK(context->transaction == nullptr);

  service_->DispatchCommand(CmdArgList{args}, context);

  DCHECK(context->transaction == nullptr);

  unique_lock lk(mu_);
  last_cmd_dbg_info_ = context->last_command_debug;

  RespVec vec = conn_wrapper->ParseResponse();
  if (vec.size() == 1)
    return vec.front();
  RespVec* new_vec = new RespVec(vec);
  resp_vec_.push_back(new_vec);
  RespExpr e;
  e.type = RespExpr::ARRAY;
  e.u = new_vec;

  return e;
}

auto BaseFamilyTest::RunMC(MP::CmdType cmd_type, string_view key, string_view value, uint32_t flags,
                           chrono::seconds ttl) -> MCResponse {
  if (!ProactorBase::IsProactorThread()) {
    return pp_->at(0)->Await([&] { return this->RunMC(cmd_type, key, value, flags, ttl); });
  }

  MP::Command cmd;
  cmd.type = cmd_type;
  cmd.key = key;
  cmd.flags = flags;
  cmd.bytes_len = value.size();
  cmd.expire_ts = ttl.count();

  TestConnWrapper* conn = AddFindConn(Protocol::MEMCACHE, GetId());

  auto* context = conn->cmd_cntx();

  DCHECK(context->transaction == nullptr);

  service_->DispatchMC(cmd, value, context);

  DCHECK(context->transaction == nullptr);

  return conn->SplitLines();
}

auto BaseFamilyTest::RunMC(MP::CmdType cmd_type, std::string_view key) -> MCResponse {
  if (!ProactorBase::IsProactorThread()) {
    return pp_->at(0)->Await([&] { return this->RunMC(cmd_type, key); });
  }

  MP::Command cmd;
  cmd.type = cmd_type;
  cmd.key = key;
  TestConnWrapper* conn = AddFindConn(Protocol::MEMCACHE, GetId());

  auto* context = conn->cmd_cntx();

  service_->DispatchMC(cmd, string_view{}, context);

  return conn->SplitLines();
}

auto BaseFamilyTest::GetMC(MP::CmdType cmd_type, std::initializer_list<std::string_view> list)
    -> MCResponse {
  CHECK_GT(list.size(), 0u);
  CHECK(base::_in(cmd_type, {MP::GET, MP::GAT, MP::GETS, MP::GATS}));

  if (!ProactorBase::IsProactorThread()) {
    return pp_->at(0)->Await([&] { return this->GetMC(cmd_type, list); });
  }

  MP::Command cmd;
  cmd.type = cmd_type;
  auto src = list.begin();
  cmd.key = *src++;
  for (; src != list.end(); ++src) {
    cmd.keys_ext.push_back(*src);
  }

  TestConnWrapper* conn = AddFindConn(Protocol::MEMCACHE, GetId());

  auto* context = conn->cmd_cntx();

  service_->DispatchMC(cmd, string_view{}, context);

  return conn->SplitLines();
}

int64_t BaseFamilyTest::CheckedInt(std::initializer_list<std::string_view> list) {
  RespExpr resp = Run(list);
  if (resp.type == RespExpr::INT64) {
    return get<int64_t>(resp.u);
  }
  if (resp.type == RespExpr::NIL) {
    return INT64_MIN;
  }

  CHECK_EQ(RespExpr::STRING, int(resp.type)) << list;
  string_view sv = ToSV(resp.GetBuf());
  int64_t res;
  CHECK(absl::SimpleAtoi(sv, &res)) << "|" << sv << "|";
  return res;
}

CmdArgVec BaseFamilyTest::TestConnWrapper::Args(ArgSlice list) {
  CHECK_NE(0u, list.size());

  CmdArgVec res;
  for (auto v : list) {
    if (v.empty()) {
      res.push_back(MutableSlice{});
    } else {
      tmp_str_vec_.emplace_back(new string{v});
      auto& s = *tmp_str_vec_.back();

      res.emplace_back(s.data(), s.size());
    }
  }

  return res;
}

RespVec BaseFamilyTest::TestConnWrapper::ParseResponse() {
  tmp_str_vec_.emplace_back(new string{sink_.str()});
  auto& s = *tmp_str_vec_.back();
  auto buf = RespExpr::buffer(&s);
  uint32_t consumed = 0;

  parser_.reset(new RedisParser{false});  // Client mode.
  RespVec res;
  RedisParser::Result st = parser_->Parse(buf, &consumed, &res);
  CHECK_EQ(RedisParser::OK, st);

  return res;
}

facade::Connection::PubMessage BaseFamilyTest::TestConnWrapper::GetPubMessage(size_t index) const {
  CHECK_LT(index, dummy_conn_->messages.size());
  return dummy_conn_->messages[index];
}

bool BaseFamilyTest::IsLocked(DbIndex db_index, std::string_view key) const {
  ShardId sid = Shard(key, shard_set->size());
  KeyLockArgs args;
  args.db_index = db_index;
  args.args = ArgSlice{&key, 1};
  args.key_step = 1;
  bool is_open = pp_->at(sid)->AwaitBrief(
      [args] { return EngineShard::tlocal()->db_slice().CheckLock(IntentLock::EXCLUSIVE, args); });
  return !is_open;
}

string BaseFamilyTest::GetId() const {
  int32 id = ProactorBase::GetIndex();
  CHECK_GE(id, 0);
  return absl::StrCat("IO", id);
}

size_t BaseFamilyTest::SubsriberMessagesLen(string_view conn_id) const {
  auto it = connections_.find(conn_id);
  if (it == connections_.end())
    return 0;

  return it->second->conn()->messages.size();
}

facade::Connection::PubMessage BaseFamilyTest::GetPublishedMessage(string_view conn_id,
                                                                   size_t index) const {
  facade::Connection::PubMessage res;

  auto it = connections_.find(conn_id);
  if (it == connections_.end())
    return res;

  return it->second->GetPubMessage(index);
}

ConnectionContext::DebugInfo BaseFamilyTest::GetDebugInfo(const std::string& id) const {
  auto it = connections_.find(id);
  CHECK(it != connections_.end());

  return it->second->cmd_cntx()->last_command_debug;
}

auto BaseFamilyTest::AddFindConn(Protocol proto, std::string_view id) -> TestConnWrapper* {
  unique_lock lk(mu_);

  auto [it, inserted] = connections_.emplace(id, nullptr);

  if (inserted) {
    it->second.reset(new TestConnWrapper(proto));
  } else {
    it->second->ClearSink();
  }
  return it->second.get();
}

vector<string> BaseFamilyTest::StrArray(const RespExpr& expr) {
  CHECK(expr.type == RespExpr::ARRAY || expr.type == RespExpr::NIL_ARRAY);
  if (expr.type == RespExpr::NIL_ARRAY)
    return vector<string>{};

  const RespVec* src = get<RespVec*>(expr.u);
  vector<string> res(src->size());
  for (size_t i = 0; i < src->size(); ++i) {
    res[i] = ToSV(src->at(i).GetBuf());
  }

  return res;
}

}  // namespace dfly
