// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/conn_context.h"

#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "util/proactor_base.h"

namespace dfly {

using namespace std;

void ConnectionContext::ChangeSubscription(bool to_add, bool to_reply, CmdArgList args) {
  vector<unsigned> result(to_reply ? args.size() : 0, 0);

  if (to_add || conn_state.subscribe_info) {
    std::vector<pair<ShardId, string_view>> channels;
    channels.reserve(args.size());

    if (!conn_state.subscribe_info) {
      DCHECK(to_add);

      conn_state.subscribe_info.reset(new ConnectionState::SubscribeInfo);
      // to be able to read input and still write the output.
      this->force_dispatch = true;
    }

    // Gather all the channels we need to subsribe to / remove.
    for (size_t i = 0; i < args.size(); ++i) {
      bool res = false;
      string_view channel = ArgS(args, i);
      if (to_add) {
        res = conn_state.subscribe_info->channels.emplace(channel).second;
      } else {
        res = conn_state.subscribe_info->channels.erase(channel) > 0;
      }

      if (to_reply)
        result[i] = conn_state.subscribe_info->channels.size();

      if (res) {
        ShardId sid = Shard(channel, shard_set->size());
        channels.emplace_back(sid, channel);
      }
    }

    if (!to_add && conn_state.subscribe_info->IsEmpty()) {
      conn_state.subscribe_info.reset();
      force_dispatch = false;
    }

    sort(channels.begin(), channels.end());

    // prepare the array in order to distribute the updates to the shards.
    vector<unsigned> shard_idx(shard_set->size() + 1, 0);
    for (const auto& k_v : channels) {
      shard_idx[k_v.first]++;
    }
    unsigned prev = shard_idx[0];
    shard_idx[0] = 0;

    // compute cumulitive sum, or in other words a beginning index in channels for each shard.
    for (size_t i = 1; i < shard_idx.size(); ++i) {
      unsigned cur = shard_idx[i];
      shard_idx[i] = shard_idx[i - 1] + prev;
      prev = cur;
    }

    int32_t tid = util::ProactorBase::GetIndex();
    DCHECK_GE(tid, 0);

    // Update the subsribers on publisher's side.
    auto cb = [&](EngineShard* shard) {
      ChannelSlice& cs = shard->channel_slice();
      unsigned start = shard_idx[shard->shard_id()];
      unsigned end = shard_idx[shard->shard_id() + 1];

      DCHECK_LT(start, end);
      for (unsigned i = start; i < end; ++i) {
        if (to_add) {
          cs.AddSubscription(channels[i].second, this, tid);
        } else {
          cs.RemoveSubscription(channels[i].second, this);
        }
      }
    };

    // Update subscription
    shard_set->RunBriefInParallel(move(cb),
                                  [&](ShardId sid) { return shard_idx[sid + 1] > shard_idx[sid]; });
  }

  if (to_reply) {
    const char* action[2] = {"unsubscribe", "subscribe"};

    for (size_t i = 0; i < result.size(); ++i) {
      (*this)->StartArray(3);
      (*this)->SendBulkString(action[to_add]);
      (*this)->SendBulkString(ArgS(args, i));  // channel

      // number of subsribed channels for this connection *right after*
      // we subsribe.
      (*this)->SendLong(result[i]);
    }
  }
}

void ConnectionContext::ChangePSub(bool to_add, bool to_reply, CmdArgList args) {
  vector<unsigned> result(to_reply ? args.size() : 0, 0);

  if (to_add || conn_state.subscribe_info) {
    std::vector<string_view> patterns;
    patterns.reserve(args.size());

    if (!conn_state.subscribe_info) {
      DCHECK(to_add);

      conn_state.subscribe_info.reset(new ConnectionState::SubscribeInfo);
      this->force_dispatch = true;
    }

    // Gather all the patterns we need to subsribe to / remove.
    for (size_t i = 0; i < args.size(); ++i) {
      bool res = false;
      string_view pattern = ArgS(args, i);
      if (to_add) {
        res = conn_state.subscribe_info->patterns.emplace(pattern).second;
      } else {
        res = conn_state.subscribe_info->patterns.erase(pattern) > 0;
      }

      if (to_reply)
        result[i] = conn_state.subscribe_info->patterns.size();

      if (res) {
        patterns.emplace_back(pattern);
      }
    }

    if (!to_add && conn_state.subscribe_info->IsEmpty()) {
      conn_state.subscribe_info.reset();
      force_dispatch = false;
    }

    int32_t tid = util::ProactorBase::GetIndex();
    DCHECK_GE(tid, 0);

    // Update the subsribers on publisher's side.
    auto cb = [&](EngineShard* shard) {
      ChannelSlice& cs = shard->channel_slice();
      for (string_view pattern : patterns) {
        if (to_add) {
          cs.AddGlobPattern(pattern, this, tid);
        } else {
          cs.RemoveGlobPattern(pattern, this);
        }
      }
    };

    // Update pattern subscription. Run on all shards.
    shard_set->RunBriefInParallel(move(cb));
  }

  if (to_reply) {
    const char* action[2] = {"punsubscribe", "psubscribe"};

    for (size_t i = 0; i < result.size(); ++i) {
      (*this)->StartArray(3);
      (*this)->SendBulkString(action[to_add]);
      (*this)->SendBulkString(ArgS(args, i));
      (*this)->SendLong(result[i]);
    }
  }
}

void ConnectionContext::OnClose() {
  if (!conn_state.subscribe_info)
    return;

  if (!conn_state.subscribe_info->channels.empty()) {
    StringVec channels(conn_state.subscribe_info->channels.begin(),
                       conn_state.subscribe_info->channels.end());
    CmdArgVec arg_vec(channels.begin(), channels.end());

    auto token = conn_state.subscribe_info->borrow_token;
    ChangeSubscription(false, false, CmdArgList{arg_vec});

    // Check that all borrowers finished processing
    token.Wait();
  }

  if (conn_state.subscribe_info) {
    DCHECK(!conn_state.subscribe_info->patterns.empty());

    StringVec patterns(conn_state.subscribe_info->patterns.begin(),
                       conn_state.subscribe_info->patterns.end());
    CmdArgVec arg_vec(patterns.begin(), patterns.end());

    auto token = conn_state.subscribe_info->borrow_token;
    ChangePSub(false, false, CmdArgList{arg_vec});

    // Check that all borrowers finished processing
    token.Wait();
    DCHECK(!conn_state.subscribe_info);
  }
}

}  // namespace dfly
