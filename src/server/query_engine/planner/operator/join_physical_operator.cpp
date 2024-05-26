#include "include/query_engine/planner/operator/join_physical_operator.h"

/* 
    TODO [Lab3] join的算子实现，需要根据join_condition实现Join的具体逻辑，
    最后将结果传递给JoinTuple, 并由current_tuple向上返回
    JoinOperator通常会遵循下面的被调用逻辑：
    operator.open()
    while(operator.next()){
        Tuple *tuple = operator.current_tuple();
    }
    operator.close()
*/

// JoinPhysicalOperator::JoinPhysicalOperator() = default;

// 执行next()前的准备工作, trx是之后事务中会使用到的，这里不用考虑
RC JoinPhysicalOperator::open(Trx *trx)
{
    vector<unique_ptr<PhysicalOperator>>& chlidrens = children();
    if (chlidrens.size() != 2)
    {
        return RC::INTERNAL;
    }

    auto left = chlidrens[0].get();
    auto right = chlidrens[1].get();
    if (left == nullptr || right == nullptr)
    {
        return RC::INTERNAL;
    }

    left->open(trx);
    // _right->open(trx);
    left_ = left;
    right_ = right;
    trx_ = trx;

    // // 首先遍历获取右侧的所有数据，防止重复遍历
    // vector<Tuple*> right_tuples;
    // while (right->next() == RC::SUCCESS) {
    //     right_tuples.push_back(right->current_tuple());
    // }

    // RC rc = RC::RECORD_EOF;
    // while (left->next() == RC::SUCCESS)
    // {
    //     Tuple *left_tuple = left->current_tuple();
    //     for (auto right_tuple : right_tuples) {
    //         // 拼接左右tuple
    //         joined_tuple_.set_left(left_tuple);
    //         joined_tuple_.set_right(right_tuple);

    //         // 判断是否符合join条件
    //         bool result = false;
    //         rc = filter(joined_tuple_, result);
    //         if (rc != RC::SUCCESS) {
    //             return rc;
    //         }

    //         if (result) {
    //             joined_tuples.push_back(joined_tuple_);
    //         }
    //     }
    // }

    return RC::SUCCESS;
}

// 计算出接下来需要输出的数据，并将结果set到join_tuple中
// 如果没有更多数据，返回RC::RECORD_EOF
RC JoinPhysicalOperator::next()
{
    // if (joined_tuples.empty()) {
    //     return RC::RECORD_EOF;
    // }
    // joined_tuple_ = joined_tuples.front();
    // joined_tuples.erase(joined_tuples.begin());

    while(true)
    {
        if(!right_opened) {
            if(left_->next() != RC::SUCCESS)
                break;
            right_->open(trx_);
            right_opened = true;
        }

        Tuple* left_tuple = left_->current_tuple();
        while(right_->next() == RC::SUCCESS)
        {
            Tuple* right_tuple = right_->current_tuple();
            joined_tuple_.set_left(left_tuple);
            joined_tuple_.set_right(right_tuple);
            bool result = false;
            RC _rc_ = filter(joined_tuple_, result);
            if (_rc_ != RC::SUCCESS) {
                return _rc_;
            }

            if (result) {
                return RC::SUCCESS;
            }
        }
        right_->close();
        right_opened = false;
    }

    return RC::RECORD_EOF;
}

// 节点执行完成，清理左右子算子
RC JoinPhysicalOperator::close()
{
    auto childrens = move(children());
    for(int i=0; i<childrens.size(); i++)
    {
        childrens[i]->close();
    }
    
    return RC::SUCCESS;
}

Tuple *JoinPhysicalOperator::current_tuple()
{
    return &joined_tuple_;
}

RC JoinPhysicalOperator::filter(Tuple &tuple, bool &result) 
{ 
    // 传入拼接好的tuple，计算是否符合join条件
    
    RC rc = RC::SUCCESS;
    Value value;
    rc = condition->get_value(tuple, value);

    if (rc != RC::SUCCESS) {
      return rc;
    }

    bool tmp_result = value.get_boolean();
    if (!tmp_result) {
      result = false;
      return rc;
    }

    result = true;
    return rc;
}
