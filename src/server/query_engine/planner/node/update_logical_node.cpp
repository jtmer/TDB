#include "include/query_engine/planner/node/update_logical_node.h"

UpdateLogicalNode::UpdateLogicalNode(Table *table,
                                     std::vector<UpdateUnit> update_units)
    : table_(table), update_units_(std::move(update_units)) {}