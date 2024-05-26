#pragma once

#include "physical_operator.h"
#include "include/query_engine/structor/tuple/join_tuple.h"

// TODO [Lab3] join算子的头文件定义，根据需要添加对应的变量和方法
class JoinPhysicalOperator : public PhysicalOperator
{
public:
    JoinPhysicalOperator(
        std::unique_ptr<Expression> _condition
    ) {
        condition = std::move(_condition);
    }

    ~JoinPhysicalOperator() override = default;

    PhysicalOperatorType type() const override
    {
        return PhysicalOperatorType::JOIN;
    }

    RC open(Trx *trx) override;
    RC next() override;
    RC close() override;
    Tuple *current_tuple() override;

    RC filter(Tuple &tuple, bool &result);

private:
    Trx *trx_ = nullptr;
    JoinedTuple joined_tuple_;  //! 当前关联的左右两个tuple(current_tuple)

    // std::vector<JoinedTuple> joined_tuples;  // 保存所有关联的tuple
    std::unique_ptr<Expression> condition;
    PhysicalOperator* left_ = nullptr;
    PhysicalOperator* right_ = nullptr;

    bool right_opened = false;
};
