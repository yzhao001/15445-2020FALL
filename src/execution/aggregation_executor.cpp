//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {
  child_->Init();
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  Tuple tuple;
  RID rid;
  try {
    while (child_->Next(&tuple, &rid)) {
      aht_.InsertCombine(MakeKey(&tuple), MakeVal(&tuple));
    }
  } catch (Exception &e) {
    Exception(ExceptionType::CHILD_EXE_FAIL, "InsertExecutor:child execute error.");
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  const AggregateKey &agg_key = aht_iterator_.Key();
  const AggregateValue &agg_val = aht_iterator_.Val();
  ++aht_iterator_;
  bool ismatch = plan_->GetHaving() == nullptr
                     ? true
                     : plan_->GetHaving()->EvaluateAggregate(agg_key.group_bys_, agg_val.aggregates_).GetAs<bool>();
  if (ismatch) {
    std::vector<Value> res;
    for (const Column &col : plan_->OutputSchema()->GetColumns()) {
      res.push_back(col.GetExpr()->EvaluateAggregate(agg_key.group_bys_, agg_val.aggregates_));
    }
    *tuple = Tuple(res, plan_->OutputSchema());
    return true;
  }
  return Next(tuple, rid);
}

}  // namespace bustub
