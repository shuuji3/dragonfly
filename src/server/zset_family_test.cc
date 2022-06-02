// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/zset_family.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;

namespace dfly {

class ZSetFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(ZSetFamilyTest, Add) {
  auto resp = Run({"zadd", "x", "1.1", "a"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"zscore", "x", "a"});
  EXPECT_THAT(resp, "1.1");

  resp = Run({"zadd", "x", "2", "a"});
  EXPECT_THAT(resp, IntArg(0));
  resp = Run({"zscore", "x", "a"});
  EXPECT_THAT(resp, "2");

  resp = Run({"zadd", "x", "ch", "3", "a"});
  EXPECT_THAT(resp, IntArg(1));
  resp = Run({"zscore", "x", "a"});
  EXPECT_EQ(resp, "3");

  resp = Run({"zcard", "x"});
  EXPECT_THAT(resp, IntArg(1));

  EXPECT_THAT(Run({"zadd", "x", "", "a"}), ErrArg("not a valid float"));

  EXPECT_THAT(Run({"zadd", "ztmp", "xx", "10", "member"}), IntArg(0));

  const char kHighPrecision[] = "0.79028573343077946";

  Run({"zadd", "zs", kHighPrecision, "a"});
  EXPECT_EQ(Run({"zscore", "zs", "a"}), "0.7902857334307795");
  EXPECT_EQ(0.79028573343077946, 0.7902857334307795);
}

TEST_F(ZSetFamilyTest, ZRem) {
  auto resp = Run({"zadd", "x", "1.1", "b", "2.1", "a"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"zrem", "x", "b", "c"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"zcard", "x"});
  EXPECT_THAT(resp, IntArg(1));
  EXPECT_THAT(Run({"zrange", "x", "0", "3", "byscore"}), "a");
  EXPECT_THAT(Run({"zrange", "x", "(-inf", "(+inf", "byscore"}), "a");
}

TEST_F(ZSetFamilyTest, ZRangeRank) {
  Run({"zadd", "x", "1.1", "a", "2.1", "b"});
  EXPECT_THAT(Run({"zrangebyscore", "x", "0", "(1.1"}), ArrLen(0));
  EXPECT_THAT(Run({"zrangebyscore", "x", "-inf", "1.1", "limit", "0", "10"}), "a");

  auto resp = Run({"zrevrangebyscore", "x", "+inf", "-inf", "limit", "0", "5"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_THAT(resp.GetVec(), ElementsAre("b", "a"));

  EXPECT_EQ(2, CheckedInt({"zcount", "x", "1.1", "2.1"}));
  EXPECT_EQ(1, CheckedInt({"zcount", "x", "(1.1", "2.1"}));
  EXPECT_EQ(0, CheckedInt({"zcount", "y", "(1.1", "2.1"}));

  EXPECT_EQ(0, CheckedInt({"zrank", "x", "a"}));
  EXPECT_EQ(1, CheckedInt({"zrank", "x", "b"}));
  EXPECT_EQ(1, CheckedInt({"zrevrank", "x", "a"}));
  EXPECT_EQ(0, CheckedInt({"zrevrank", "x", "b"}));
  EXPECT_THAT(Run({"zrevrank", "x", "c"}), ArgType(RespExpr::NIL));
  EXPECT_THAT(Run({"zrank", "y", "c"}), ArgType(RespExpr::NIL));
}

TEST_F(ZSetFamilyTest, ZRemRangeRank) {
  Run({"zadd", "x", "1.1", "a", "2.1", "b"});
  EXPECT_THAT(Run({"ZREMRANGEBYRANK", "y", "0", "1"}), IntArg(0));
  EXPECT_THAT(Run({"ZREMRANGEBYRANK", "x", "0", "0"}), IntArg(1));
  EXPECT_EQ(Run({"zrange", "x", "0", "5"}), "b");
  EXPECT_THAT(Run({"ZREMRANGEBYRANK", "x", "0", "1"}), IntArg(1));
  EXPECT_EQ(Run({"type", "x"}), "none");
}

TEST_F(ZSetFamilyTest, ZRemRangeScore) {
  Run({"zadd", "x", "1.1", "a", "2.1", "b"});
  EXPECT_THAT(Run({"ZREMRANGEBYSCORE", "y", "0", "1"}), IntArg(0));
  EXPECT_THAT(Run({"ZREMRANGEBYSCORE", "x", "-inf", "1.1"}), IntArg(1));
  EXPECT_EQ(Run({"zrange", "x", "0", "5"}), "b");
  EXPECT_THAT(Run({"ZREMRANGEBYSCORE", "x", "(2.0", "+inf"}), IntArg(1));
  EXPECT_EQ(Run({"type", "x"}), "none");
  EXPECT_THAT(Run({"zremrangebyscore", "x", "1", "NaN"}), ErrArg("min or max is not a float"));
}

TEST_F(ZSetFamilyTest, IncrBy) {
  auto resp = Run({"zadd", "key", "xx", "incr", "2.1", "member"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"zadd", "key", "nx", "incr", "2.1", "member"});
  EXPECT_THAT(resp, "2.1");

  resp = Run({"zadd", "key", "nx", "incr", "4.9", "member"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));
}

TEST_F(ZSetFamilyTest, ByLex) {
  Run({
      "zadd", "key",      "0", "alpha", "0", "bar",   "0", "cool", "0", "down",
      "0",    "elephant", "0", "foo",   "0", "great", "0", "hill", "0", "omega",
  });

  auto resp = Run({"zrangebylex", "key", "-", "[cool"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre("alpha", "bar", "cool"));

  EXPECT_EQ(3, CheckedInt({"ZLEXCOUNT", "key", "(foo", "+"}));
  EXPECT_EQ(3, CheckedInt({"ZREMRANGEBYLEX", "key", "(foo", "+"}));

  resp = Run({"zrangebylex", "key", "[a", "+"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_THAT(resp.GetVec(), ElementsAre("alpha", "bar", "cool", "down", "elephant", "foo"));
}

TEST_F(ZSetFamilyTest, ZRevRange) {
  Run({"zadd", "key", "-inf", "a", "1", "b", "2", "c"});
  auto resp = Run({"zrevrangebyscore", "key", "2", "-inf"});
  ASSERT_THAT(resp, ArrLen(3));
  EXPECT_THAT(resp.GetVec(), ElementsAre("c", "b", "a"));

  resp = Run({"zrevrangebyscore", "key", "2", "-inf", "withscores"});
  ASSERT_THAT(resp, ArrLen(6));
  EXPECT_THAT(resp.GetVec(), ElementsAre("c", "2", "b", "1", "a", "-inf"));
}

TEST_F(ZSetFamilyTest, ZScan) {
  string prefix(128, 'a');
  for (unsigned i = 0; i < 100; ++i) {
    Run({"zadd", "key", "1", absl::StrCat(prefix, i)});
  }

  EXPECT_EQ(100, CheckedInt({"zcard", "key"}));
  int64_t cursor = 0;
  size_t scan_len = 0;
  do {
    auto resp = Run({"zscan", "key", absl::StrCat(cursor)});
    ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
    ASSERT_THAT(resp.GetVec(), ElementsAre(ArgType(RespExpr::STRING), ArgType(RespExpr::ARRAY)));
    string_view token = ToSV(resp.GetVec()[0].GetBuf());
    ASSERT_TRUE(absl::SimpleAtoi(token, &cursor));
    auto sub_arr = resp.GetVec()[1].GetVec();
    scan_len += sub_arr.size();
  } while (cursor != 0);

  EXPECT_EQ(100 * 2, scan_len);
}

TEST_F(ZSetFamilyTest, ZUnionStore) {
  RespExpr resp;

  resp = Run({"zunionstore", "key", "0"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"zunionstore", "key", "0", "aggregate", "sum"});
  EXPECT_THAT(resp, ErrArg("at least 1 input key is needed"));
  resp = Run({"zunionstore", "key", "-1", "aggregate", "sum"});
  EXPECT_THAT(resp, ErrArg("out of range"));
  resp = Run({"zunionstore", "key", "2", "foo", "bar", "weights", "1"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  EXPECT_EQ(2, CheckedInt({"zadd", "z1", "1", "a", "2", "b"}));
  EXPECT_EQ(2, CheckedInt({"zadd", "z2", "3", "c", "2", "b"}));

  resp = Run({"zunionstore", "key", "2", "z1", "z2"});
  EXPECT_THAT(resp, IntArg(3));
  resp = Run({"zrange", "key", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "c", "3", "b", "4"));

  resp = Run({"zunionstore", "z1", "1", "z1"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"zunionstore", "z1", "2", "z1", "z2"});
  EXPECT_THAT(resp, IntArg(3));
  resp = Run({"zrange", "z1", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "c", "3", "b", "4"));

  Run({"set", "foo", "bar"});
  resp = Run({"zunionstore", "foo", "1", "z2"});
  EXPECT_THAT(resp, IntArg(2));
  resp = Run({"zrange", "foo", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("b", "2", "c", "3"));
}

TEST_F(ZSetFamilyTest, ZUnionStoreOpts) {
  EXPECT_EQ(2, CheckedInt({"zadd", "z1", "1", "a", "2", "b"}));
  EXPECT_EQ(2, CheckedInt({"zadd", "z2", "3", "c", "2", "b"}));
  RespExpr resp;

  EXPECT_EQ(3, CheckedInt({"zunionstore", "a", "2", "z1", "z2", "weights", "1", "3"}));
  resp = Run({"zrange", "a", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "b", "8", "c", "9"));

  resp = Run({"zunionstore", "a", "2", "z1", "z2", "weights", "1"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"zunionstore", "z1", "1", "z1", "weights", "2"});
  EXPECT_THAT(resp, IntArg(2));
  resp = Run({"zrange", "z1", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "2", "b", "4"));

  resp = Run({"zunionstore", "max", "2", "z1", "z2", "weights", "1", "0", "aggregate", "max"});
  ASSERT_THAT(resp, IntArg(3));
  resp = Run({"zrange", "max", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("c", "0", "a", "2", "b", "4"));
}

TEST_F(ZSetFamilyTest, ZInterStore) {
  EXPECT_EQ(2, CheckedInt({"zadd", "z1", "1", "a", "2", "b"}));
  EXPECT_EQ(2, CheckedInt({"zadd", "z2", "3", "c", "2", "b"}));
  RespExpr resp;

  EXPECT_EQ(1, CheckedInt({"zinterstore", "a", "2", "z1", "z2"}));
  resp = Run({"zrange", "a", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("b", "4"));
}

}  // namespace dfly
