use crate::OptimizerError;
use crate::cost::model::cost_physical_op;
use crate::cost::stats::CatalogSnapshot;
use crate::ir::operator::Operator;
use crate::ir::types::Cost;
use crate::memo::group::Winner;
use crate::memo::{ExprId, GroupId, Memo};
use crate::properties::delivered::DeliveredProperties;
use crate::properties::required::{RequiredProperties, RequiredPropsKey};

use super::engine::{derive_props, get_page_count, SearchCtx, MAX_GROUPS};
use super::task::{
    DeriveLogicalPropsState, OptimizeExprState, OptimizeGroupState, Task,
};

pub struct Scheduler {
    task_stack: Vec<Task>,
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            task_stack: Vec::new(),
        }
    }

    pub fn schedule(&mut self, task: Task) {
        self.task_stack.push(task);
    }

    pub(super) fn run(
        &mut self,
        memo: &mut Memo,
        catalog: &CatalogSnapshot,
        ctx: &mut SearchCtx,
    ) -> Result<(), OptimizerError> {
        while let Some(task) = self.task_stack.pop() {
            match task {
                Task::OptimizeGroup {
                    group_id,
                    required,
                    upper_bound,
                    state,
                } => self.execute_optimize_group(group_id, required, upper_bound, state, memo, catalog, ctx),
                Task::OptimizeExpr {
                    group_id,
                    expr_id,
                    required,
                    upper_bound,
                    state,
                } => self.execute_optimize_expr(group_id, expr_id, required, upper_bound, state, memo, catalog, ctx),
                Task::DeriveLogicalProps { group_id, state } => {
                    self.execute_derive_logical_props(group_id, state, memo, catalog)
                }
            }
        }
        Ok(())
    }

    fn execute_optimize_group(
        &mut self,
        group_id: GroupId,
        required: RequiredProperties,
        upper_bound: f64,
        state: OptimizeGroupState,
        memo: &mut Memo,
        catalog: &CatalogSnapshot,
        ctx: &mut SearchCtx,
    ) {
        match state {
            OptimizeGroupState::Init => {
                let key = RequiredPropsKey::from(&required);
                if let Some(winner) = memo.get_group(group_id).winners.get(&key) {
                    if winner.cost.total <= upper_bound {
                        return;
                    }
                }

                if memo.get_group(group_id).logical_props.get().is_none() {
                    // Need to derive properties first. Re-push self, then push derive task.
                    self.schedule(Task::OptimizeGroup {
                        group_id,
                        required,
                        upper_bound,
                        state: OptimizeGroupState::Init,
                    });
                    self.schedule(Task::DeriveLogicalProps {
                        group_id,
                        state: DeriveLogicalPropsState::Init,
                    });
                    return;
                }

                // Explore & Implement
                if !memo.get_group(group_id).explored && !ctx.check_timeout() {
                    let expr_ids = memo.get_group(group_id).exprs.clone();
                    for eid in expr_ids {
                        for rule in &ctx.rules.xform_rules {
                            let expr = memo.get_expr(eid);
                            if rule.matches(expr, memo) {
                                rule.apply(eid, group_id, memo, catalog);
                            }
                        }
                    }
                    memo.get_group_mut(group_id).explored = true;
                }

                if !memo.get_group(group_id).implemented {
                    let expr_ids = memo.get_group(group_id).exprs.clone();
                    for eid in expr_ids {
                        let expr = memo.get_expr(eid);
                        if expr.op.is_logical() {
                            let matching: Vec<usize> = ctx
                                .rules
                                .impl_rules
                                .iter()
                                .enumerate()
                                .filter(|(_, rule)| rule.matches(memo.get_expr(eid), memo))
                                .map(|(i, _)| i)
                                .collect();
                            for rule_idx in matching {
                                ctx.rules.impl_rules[rule_idx].apply(eid, group_id, memo, catalog);
                            }
                        }
                    }
                    memo.get_group_mut(group_id).implemented = true;
                }

                if memo.group_count() > MAX_GROUPS {
                    ctx.timed_out = true;
                }

                self.schedule(Task::OptimizeGroup {
                    group_id,
                    required,
                    upper_bound,
                    state: OptimizeGroupState::OptimizingExprs { expr_idx: 0 },
                });
            }
            OptimizeGroupState::OptimizingExprs { expr_idx } => {
                let exprs = memo.get_group(group_id).exprs.clone();
                let mut next_idx = expr_idx;
                while next_idx < exprs.len() && !memo.get_expr(exprs[next_idx]).op.is_physical() {
                    next_idx += 1;
                }

                if next_idx < exprs.len() {
                    let eid = exprs[next_idx];
                    let key = RequiredPropsKey::from(&required);
                    let current_best = memo
                        .get_group(group_id)
                        .winners
                        .get(&key)
                        .map(|w| w.cost.total)
                        .unwrap_or(upper_bound);

                    self.schedule(Task::OptimizeGroup {
                        group_id,
                        required: required.clone(),
                        upper_bound: current_best,
                        state: OptimizeGroupState::OptimizingExprs {
                            expr_idx: next_idx + 1,
                        },
                    });

                    self.schedule(Task::OptimizeExpr {
                        group_id,
                        expr_id: eid,
                        required,
                        upper_bound: current_best,
                        state: OptimizeExprState::Init,
                    });
                }
            }
        }
    }

    fn execute_optimize_expr(
        &mut self,
        group_id: GroupId,
        expr_id: ExprId,
        required: RequiredProperties,
        upper_bound: f64,
        state: OptimizeExprState,
        memo: &mut Memo,
        catalog: &CatalogSnapshot,
        _ctx: &mut SearchCtx,
    ) {
        match state {
            OptimizeExprState::Init => {
                let expr = memo.get_expr(expr_id);
                if expr.children.is_empty() {
                    self.cost_and_update_winner(
                        group_id,
                        expr_id,
                        required,
                        upper_bound,
                        vec![],
                        vec![],
                        memo,
                        catalog,
                    );
                } else {
                    self.schedule(Task::OptimizeExpr {
                        group_id,
                        expr_id,
                        required,
                        upper_bound,
                        state: OptimizeExprState::WaitingForChild {
                            child_idx: 0,
                            accumulated_cost: 0.0,
                            child_costs: vec![],
                            child_rows: vec![],
                        },
                    });
                    self.schedule(Task::OptimizeGroup {
                        group_id: expr.children[0],
                        required: RequiredProperties::none(),
                        upper_bound,
                        state: OptimizeGroupState::Init,
                    });
                }
            }
            OptimizeExprState::WaitingForChild {
                child_idx,
                accumulated_cost,
                mut child_costs,
                mut child_rows,
            } => {
                let expr = memo.get_expr(expr_id);
                let child_gid = expr.children[child_idx];
                let key = RequiredPropsKey::from(&RequiredProperties::none());

                if let Some(winner) = memo.get_group(child_gid).winners.get(&key) {
                    let cost = winner.cost.total;
                    if accumulated_cost + cost > upper_bound {
                        // pruned
                        return;
                    }
                    child_costs.push(cost);
                    let rows = memo
                        .get_group(child_gid)
                        .logical_props
                        .get()
                        .map(|p| p.row_count)
                        .unwrap_or(1000.0);
                    child_rows.push(rows);

                    if child_idx + 1 == expr.children.len() {
                        self.cost_and_update_winner(
                            group_id,
                            expr_id,
                            required,
                            upper_bound,
                            child_costs,
                            child_rows,
                            memo,
                            catalog,
                        );
                    } else {
                        self.schedule(Task::OptimizeExpr {
                            group_id,
                            expr_id,
                            required,
                            upper_bound,
                            state: OptimizeExprState::WaitingForChild {
                                child_idx: child_idx + 1,
                                accumulated_cost: accumulated_cost + cost,
                                child_costs,
                                child_rows,
                            },
                        });
                        self.schedule(Task::OptimizeGroup {
                            group_id: expr.children[child_idx + 1],
                            required: RequiredProperties::none(),
                            upper_bound: upper_bound - accumulated_cost - cost,
                            state: OptimizeGroupState::Init,
                        });
                    }
                }
            }
        }
    }

    fn execute_derive_logical_props(
        &mut self,
        group_id: GroupId,
        state: DeriveLogicalPropsState,
        memo: &mut Memo,
        catalog: &CatalogSnapshot,
    ) {
        match state {
            DeriveLogicalPropsState::Init => {
                if memo.get_group(group_id).logical_props.get().is_some() {
                    return;
                }
                let exprs = memo.get_group(group_id).exprs.clone();
                if let Some(&eid) = exprs.iter().find(|&&e| memo.get_expr(e).op.is_logical()) {
                    let expr = memo.get_expr(eid);
                    if expr.children.is_empty() {
                        let logical_op = match &expr.op {
                            Operator::Logical(op) => op,
                            _ => unreachable!(),
                        };
                        let props = derive_props(logical_op, &[], memo, catalog);
                        let _ = memo.get_group(group_id).logical_props.set(props);
                    } else {
                        self.schedule(Task::DeriveLogicalProps {
                            group_id,
                            state: DeriveLogicalPropsState::WaitingForChildren {
                                expr_id: eid,
                                child_idx: 0,
                            },
                        });
                        self.schedule(Task::DeriveLogicalProps {
                            group_id: expr.children[0],
                            state: DeriveLogicalPropsState::Init,
                        });
                    }
                }
            }
            DeriveLogicalPropsState::WaitingForChildren { expr_id, child_idx } => {
                let expr = memo.get_expr(expr_id);
                if child_idx + 1 == expr.children.len() {
                    let logical_op = match &expr.op {
                        Operator::Logical(op) => op,
                        _ => unreachable!(),
                    };
                    let props = derive_props(logical_op, &expr.children, memo, catalog);
                    let _ = memo.get_group(group_id).logical_props.set(props);
                } else {
                    self.schedule(Task::DeriveLogicalProps {
                        group_id,
                        state: DeriveLogicalPropsState::WaitingForChildren {
                            expr_id,
                            child_idx: child_idx + 1,
                        },
                    });
                    self.schedule(Task::DeriveLogicalProps {
                        group_id: expr.children[child_idx + 1],
                        state: DeriveLogicalPropsState::Init,
                    });
                }
            }
        }
    }

    fn cost_and_update_winner(
        &self,
        group_id: GroupId,
        expr_id: ExprId,
        required: RequiredProperties,
        upper_bound: f64,
        child_costs: Vec<f64>,
        child_rows: Vec<f64>,
        memo: &mut Memo,
        catalog: &CatalogSnapshot,
    ) {
        let expr = memo.get_expr(expr_id);
        let phys_op = match &expr.op {
            Operator::Physical(p) => p.clone(),
            _ => return,
        };

        let logical_props = memo
            .get_group(group_id)
            .logical_props
            .get()
            .cloned()
            .unwrap_or_default();

        let children_costs: Vec<Cost> = child_costs
            .into_iter()
            .map(|c| Cost {
                startup: 0.0,
                total: c,
            })
            .collect();

        let page_count = get_page_count(&phys_op, catalog);

        let local_cost = cost_physical_op(
            &phys_op,
            &logical_props,
            &catalog.cost_params,
            &children_costs,
            &child_rows,
            page_count,
        );

        let total_cost = local_cost.total;

        if total_cost < upper_bound {
            let key = RequiredPropsKey::from(&required);
            // double check to ensure we only write if we are still the best
            let current_best = memo.get_group(group_id).winners.get(&key).map(|w| w.cost.total).unwrap_or(upper_bound);
            if total_cost < current_best {
                memo.get_group_mut(group_id).winners.insert(
                    key,
                    Winner {
                        expr_id,
                        cost: Cost {
                            startup: local_cost.startup,
                            total: total_cost,
                        },
                        delivered_props: DeliveredProperties::none(),
                    },
                );
            }
        }
    }
}
