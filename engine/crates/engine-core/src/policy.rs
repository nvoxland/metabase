//! The analyzer rule that will inject row filters, column masks, and table visibility from
//! the session context. Identity in v1: the hook exists so the pipeline shape is final.

use std::fmt::Debug;

use datafusion::common::config::ConfigOptions;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::AnalyzerRule;

use crate::api::SessionContext;

#[derive(Debug)]
pub struct PolicyInjectionRule {
    #[allow(dead_code)]
    context: SessionContext,
}

impl PolicyInjectionRule {
    pub fn new(context: SessionContext) -> PolicyInjectionRule {
        PolicyInjectionRule { context }
    }
}

impl AnalyzerRule for PolicyInjectionRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        Ok(plan)
    }

    fn name(&self) -> &str {
        "metabase_policy_injection"
    }
}
