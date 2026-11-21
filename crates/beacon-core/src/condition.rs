//! The typed condition tree (spec §4.3).
//!
//! Semantics:
//! - Operators: `eq, neq, gt, gte, lt, lte, in, not_in, contains, starts_with,
//!   regex, exists, not_exists`, nested under `all` / `any` / `not`.
//! - Missing is not JSON `null`. Any comparison on a missing or incompatible
//!   value is **false**, including `neq` and `not_in`. `not` negates that.
//! - `exists` is true for a present `null`.
//! - Regexes are compiled once, at publish time; a malformed one is an error.

use std::cmp::Ordering;

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::event::Event;
use crate::path::{Path, PathError, Root};

/// Upper bound on tree size, so a pasted rule can't blow up evaluation.
pub const MAX_NODES: usize = 256;
pub const MAX_REGEX_LEN: usize = 1024;
/// Upper bound on `in` / `not_in` lists.
pub const MAX_LIST: usize = 256;
/// Bound compiled NFA/DFA size so short regexes like `(a{50}){50}` cannot blow up memory.
const MAX_REGEX_SIZE_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Op {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
    NotIn,
    Contains,
    StartsWith,
    Regex,
    Exists,
    NotExists,
}

/// The YAML/JSON form of a condition, as written by people or the rule builder.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConditionSpec {
    All {
        all: Vec<ConditionSpec>,
    },
    Any {
        any: Vec<ConditionSpec>,
    },
    Not {
        not: Box<ConditionSpec>,
    },
    Leaf {
        path: String,
        op: Op,
        /// `Some(Null)` for an explicit `value: null`, `None` when absent.
        #[serde(default, deserialize_with = "present", skip_serializing_if = "Option::is_none")]
        value: Option<Value>,
    },
}

fn present<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(d).map(Some)
}

/// A compiled condition: paths parsed, regexes compiled, literals checked.
#[derive(Debug, Clone)]
pub enum Condition {
    All(Vec<Condition>),
    Any(Vec<Condition>),
    Not(Box<Condition>),
    Leaf(Leaf),
}

#[derive(Debug, Clone)]
pub struct Leaf {
    pub path: Path,
    pub op: Op,
    pub value: Value,
    regex: Option<Regex>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConditionError {
    #[error("invalid path: {0}")]
    Path(#[from] PathError),
    #[error("`{op:?}` on `{path}` needs a `value`")]
    MissingValue { op: Op, path: String },
    #[error("`{op:?}` on `{path}` needs a list `value`")]
    NeedsList { op: Op, path: String },
    #[error("`{op:?}` on `{path}` needs a string `value`")]
    NeedsString { op: Op, path: String },
    #[error("invalid regex on `{path}`: {source}")]
    Regex {
        path: String,
        #[source]
        source: regex::Error,
    },
    #[error("regex on `{path}` is longer than {MAX_REGEX_LEN} characters")]
    RegexTooLong { path: String },
    #[error("condition has more than {MAX_NODES} nodes")]
    TooLarge,
    #[error("`all`/`any` must not be empty")]
    EmptyGroup,
}

impl Condition {
    /// Compile with fully-qualified paths (rules).
    pub fn compile(spec: &ConditionSpec) -> Result<Condition, ConditionError> {
        let mut budget = MAX_NODES;
        compile(spec, None, &mut budget)
    }

    /// Compile where unqualified paths are relative to `root` (schema detectors).
    pub fn compile_with_default_root(
        spec: &ConditionSpec,
        root: Root,
    ) -> Result<Condition, ConditionError> {
        let mut budget = MAX_NODES;
        compile(spec, Some(root), &mut budget)
    }

    pub fn eval(&self, event: &Event) -> bool {
        match self {
            Condition::All(cs) => cs.iter().all(|c| c.eval(event)),
            Condition::Any(cs) => cs.iter().any(|c| c.eval(event)),
            Condition::Not(c) => !c.eval(event),
            Condition::Leaf(leaf) => leaf.eval(event),
        }
    }

    /// Evaluate and report each node's result, in pre-order. Used by the rule
    /// test panel so people can see which condition failed.
    pub fn explain(&self, event: &Event) -> Vec<NodeResult> {
        let mut out = Vec::new();
        self.explain_into(event, 0, &mut out);
        out
    }

    fn explain_into(&self, event: &Event, depth: usize, out: &mut Vec<NodeResult>) -> bool {
        let idx = out.len();
        out.push(NodeResult {
            depth,
            label: String::new(),
            actual: None,
            result: false,
        });
        let (label, actual, result) = match self {
            Condition::All(cs) => {
                let mut all = true;
                for c in cs {
                    all &= c.explain_into(event, depth + 1, out);
                }
                ("all".to_string(), None, all)
            }
            Condition::Any(cs) => {
                let mut any = false;
                for c in cs {
                    any |= c.explain_into(event, depth + 1, out);
                }
                ("any".to_string(), None, any)
            }
            Condition::Not(c) => {
                let r = !c.explain_into(event, depth + 1, out);
                ("not".to_string(), None, r)
            }
            Condition::Leaf(leaf) => {
                let actual = leaf.path.resolve(event).map(|v| v.into_owned());
                let label = format!("{} {:?} {}", leaf.path, leaf.op, leaf.value);
                (label, actual, leaf.eval(event))
            }
        };
        out[idx].label = label;
        out[idx].actual = actual;
        out[idx].result = result;
        result
    }
}

/// One node of the rule test panel: `node` is the child-index path from the
/// root (`""` = root, `"1.0"` = first child of the second child; a `not`
/// wraps its child at index 0). After an `all`/`any` is decided, the remaining
/// siblings and their subtrees are `skipped`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TestNode {
    pub node: String,
    pub result: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual: Option<Value>,
}

impl Condition {
    pub fn test_nodes(&self, event: &Event) -> Vec<TestNode> {
        let mut out = Vec::new();
        self.test_into(event, String::new(), false, &mut out);
        out
    }

    fn test_into(&self, event: &Event, path: String, skip: bool, out: &mut Vec<TestNode>) -> bool {
        let child = |i: usize| if path.is_empty() { i.to_string() } else { format!("{path}.{i}") };
        let idx = out.len();
        out.push(TestNode { node: path.clone(), result: "skipped", actual: None });
        if skip {
            match self {
                Condition::All(cs) | Condition::Any(cs) => {
                    for (i, c) in cs.iter().enumerate() {
                        c.test_into(event, child(i), true, out);
                    }
                }
                Condition::Not(c) => {
                    c.test_into(event, child(0), true, out);
                }
                Condition::Leaf(_) => {}
            }
            return false;
        }
        let result = match self {
            Condition::All(cs) => {
                let mut decided = false;
                for (i, c) in cs.iter().enumerate() {
                    let r = c.test_into(event, child(i), decided, out);
                    if !decided && !r {
                        decided = true;
                    }
                }
                !decided
            }
            Condition::Any(cs) => {
                let mut decided = false;
                for (i, c) in cs.iter().enumerate() {
                    let r = c.test_into(event, child(i), decided, out);
                    if !decided && r {
                        decided = true;
                    }
                }
                decided
            }
            Condition::Not(c) => !c.test_into(event, child(0), false, out),
            Condition::Leaf(leaf) => {
                out[idx].actual = leaf.path.resolve(event).map(|v| crate::matcher::bounded_to(v.into_owned(), 1024));
                leaf.eval(event)
            }
        };
        out[idx].result = if result { "match" } else { "no_match" };
        result
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeResult {
    pub depth: usize,
    pub label: String,
    pub actual: Option<Value>,
    pub result: bool,
}

fn compile(
    spec: &ConditionSpec,
    default_root: Option<Root>,
    budget: &mut usize,
) -> Result<Condition, ConditionError> {
    if *budget == 0 {
        return Err(ConditionError::TooLarge);
    }
    *budget -= 1;
    Ok(match spec {
        ConditionSpec::All { all } => {
            if all.is_empty() {
                return Err(ConditionError::EmptyGroup);
            }
            Condition::All(
                all.iter()
                    .map(|c| compile(c, default_root, budget))
                    .collect::<Result<_, _>>()?,
            )
        }
        ConditionSpec::Any { any } => {
            if any.is_empty() {
                return Err(ConditionError::EmptyGroup);
            }
            Condition::Any(
                any.iter()
                    .map(|c| compile(c, default_root, budget))
                    .collect::<Result<_, _>>()?,
            )
        }
        ConditionSpec::Not { not } => Condition::Not(Box::new(compile(not, default_root, budget)?)),
        ConditionSpec::Leaf { path, op, value } => {
            let path = match default_root {
                Some(root) => Path::parse_with_default_root(path, root)?,
                None => Path::parse(path)?,
            };
            let p = path.as_str().to_string();
            let needs_value = !matches!(op, Op::Exists | Op::NotExists);
            let value = match (value, needs_value) {
                (Some(v), _) => v.clone(),
                (None, false) => Value::Null,
                (None, true) => return Err(ConditionError::MissingValue { op: *op, path: p }),
            };
            let mut regex = None;
            match op {
                Op::In | Op::NotIn if !value.is_array() => {
                    return Err(ConditionError::NeedsList { op: *op, path: p });
                }
                Op::In | Op::NotIn if value.as_array().is_some_and(|a| a.len() > MAX_LIST) => {
                    return Err(ConditionError::TooLarge);
                }
                Op::StartsWith if !value.is_string() => {
                    return Err(ConditionError::NeedsString { op: *op, path: p });
                }
                Op::Regex => {
                    let pat = value
                        .as_str()
                        .ok_or(ConditionError::NeedsString { op: *op, path: p.clone() })?;
                    if pat.len() > MAX_REGEX_LEN {
                        return Err(ConditionError::RegexTooLong { path: p });
                    }
                    regex = Some(
                        regex::RegexBuilder::new(pat)
                            .size_limit(MAX_REGEX_SIZE_BYTES)
                            .dfa_size_limit(MAX_REGEX_SIZE_BYTES)
                            .build()
                            .map_err(|source| ConditionError::Regex { path: p, source })?,
                    );
                }
                _ => {}
            }
            Condition::Leaf(Leaf {
                path,
                op: *op,
                value,
                regex,
            })
        }
    })
}

impl Leaf {
    pub fn eval(&self, event: &Event) -> bool {
        let actual = self.path.resolve(event);
        match self.op {
            Op::Exists => actual.is_some(),
            Op::NotExists => actual.is_none(),
            _ => match actual {
                None => false,
                Some(actual) => self.compare(actual.as_ref()),
            },
        }
    }

    fn compare(&self, actual: &Value) -> bool {
        let expected = &self.value;
        match self.op {
            Op::Eq => json_eq(actual, expected),
            Op::Neq => comparable(actual, expected) && !json_eq(actual, expected),
            Op::Gt => ordering(actual, expected) == Some(Ordering::Greater),
            Op::Gte => matches!(ordering(actual, expected), Some(Ordering::Greater | Ordering::Equal)),
            Op::Lt => ordering(actual, expected) == Some(Ordering::Less),
            Op::Lte => matches!(ordering(actual, expected), Some(Ordering::Less | Ordering::Equal)),
            Op::In => list(expected).iter().any(|e| json_eq(actual, e)),
            Op::NotIn => {
                let items = list(expected);
                items.iter().any(|e| comparable(actual, e)) && !items.iter().any(|e| json_eq(actual, e))
            }
            Op::Contains => match (actual, expected) {
                (Value::String(a), Value::String(e)) => a.contains(e.as_str()),
                (Value::Array(items), e) => items.iter().any(|i| json_eq(i, e)),
                _ => false,
            },
            Op::StartsWith => match (actual, expected) {
                (Value::String(a), Value::String(e)) => a.starts_with(e.as_str()),
                _ => false,
            },
            Op::Regex => match (actual, &self.regex) {
                (Value::String(a), Some(re)) => re.is_match(a),
                _ => false,
            },
            Op::Exists | Op::NotExists => unreachable!("handled in eval"),
        }
    }
}

fn list(v: &Value) -> &[Value] {
    v.as_array().map(Vec::as_slice).unwrap_or(&[])
}

/// Two values are comparable if they have the same JSON type (numbers are one type).
fn comparable(a: &Value, b: &Value) -> bool {
    matches!(
        (a, b),
        (Value::Null, Value::Null)
            | (Value::Bool(_), Value::Bool(_))
            | (Value::Number(_), Value::Number(_))
            | (Value::String(_), Value::String(_))
            | (Value::Array(_), Value::Array(_))
            | (Value::Object(_), Value::Object(_))
    )
}

/// JSON equality with numeric normalization (`1 == 1.0`).
fn json_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => num_cmp(x, y) == Some(Ordering::Equal),
        _ => a == b,
    }
}

fn ordering(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => num_cmp(x, y),
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// Exact for integers (i64/u64), f64 otherwise.
fn num_cmp(x: &serde_json::Number, y: &serde_json::Number) -> Option<Ordering> {
    if let (Some(a), Some(b)) = (x.as_i64(), y.as_i64()) {
        return Some(a.cmp(&b));
    }
    if let (Some(a), Some(b)) = (x.as_u64(), y.as_u64()) {
        return Some(a.cmp(&b));
    }
    x.as_f64()?.partial_cmp(&y.as_f64()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ev(raw: Value) -> Event {
        Event::new("e1".into(), "kafka:trades".into(), raw)
    }

    fn cond(yaml: &str) -> Condition {
        let spec: ConditionSpec = serde_saphyr::from_str(yaml).unwrap();
        Condition::compile(&spec).unwrap()
    }

    fn trade() -> Event {
        ev(json!({
            "meta": {"schema": "trade.rejected.v1", "producer": "trade-gateway"},
            "payload": {"tradeId": "T-99877", "desk": "rates", "notional": 25000000, "currency": "EUR", "reason": "LIMIT_BREACH", "tags": ["a", "b"], "maybe": null}
        }))
    }

    #[test]
    fn rates_rule_matches() {
        let c = cond(
            r#"
all:
  - { path: raw.meta.schema, op: eq, value: "trade.rejected.v1" }
  - { path: raw.payload.desk, op: eq, value: rates }
  - { path: raw.payload.notional, op: gt, value: 10000000 }
"#,
        );
        assert!(c.eval(&trade()));
    }

    #[test]
    fn threshold_equality_does_not_match_gt() {
        let c = cond("{ path: raw.payload.notional, op: gt, value: 25000000 }");
        assert!(!c.eval(&trade()));
        let c = cond("{ path: raw.payload.notional, op: gte, value: 25000000.0 }");
        assert!(c.eval(&trade()));
    }

    #[test]
    fn missing_compares_false_even_for_negations() {
        let e = trade();
        assert!(!cond("{ path: raw.nope, op: eq, value: 1 }").eval(&e));
        assert!(!cond("{ path: raw.nope, op: neq, value: 1 }").eval(&e));
        assert!(!cond("{ path: raw.nope, op: not_in, value: [1, 2] }").eval(&e));
        assert!(cond("{ not: { path: raw.nope, op: eq, value: 1 } }").eval(&e));
        assert!(cond("{ path: raw.nope, op: not_exists }").eval(&e));
    }

    #[test]
    fn incompatible_types_compare_false() {
        let e = trade();
        assert!(!cond("{ path: raw.payload.desk, op: gt, value: 5 }").eval(&e));
        assert!(!cond("{ path: raw.payload.desk, op: neq, value: 5 }").eval(&e));
        assert!(cond("{ path: raw.payload.desk, op: neq, value: fx }").eval(&e));
    }

    #[test]
    fn null_exists() {
        let e = trade();
        assert!(cond("{ path: raw.payload.maybe, op: exists }").eval(&e));
        assert!(cond("{ path: raw.payload.maybe, op: eq, value: null }").eval(&e));
    }

    #[test]
    fn list_and_string_ops() {
        let e = trade();
        assert!(cond("{ path: raw.payload.reason, op: in, value: [TIMEOUT, LIMIT_BREACH] }").eval(&e));
        assert!(cond("{ path: raw.payload.reason, op: not_in, value: [TIMEOUT] }").eval(&e));
        assert!(cond("{ path: raw.payload.tags, op: contains, value: b }").eval(&e));
        assert!(cond("{ path: raw.payload.reason, op: contains, value: BREACH }").eval(&e));
        assert!(cond("{ path: raw.payload.tradeId, op: starts_with, value: T- }").eval(&e));
        assert!(cond(r#"{ path: raw.payload.tradeId, op: regex, value: "^T-\\d+$" }"#).eval(&e));
    }

    #[test]
    fn any_and_not() {
        let e = trade();
        assert!(cond("any: [ { path: raw.payload.desk, op: eq, value: fx }, { path: raw.payload.desk, op: eq, value: rates } ]").eval(&e));
        assert!(!cond("not: { path: raw.payload.desk, op: eq, value: rates }").eval(&e));
    }

    #[test]
    fn compile_errors() {
        let bad = |y: &str| Condition::compile(&serde_saphyr::from_str::<ConditionSpec>(y).unwrap()).is_err();
        assert!(bad("{ path: raw.x, op: regex, value: \"(\" }"));
        assert!(bad("{ path: raw.x, op: regex, value: \"(a{500}){500}\" }"));
        assert!(bad("{ path: raw.x, op: eq }"));
        assert!(bad("{ path: raw.x, op: in, value: 3 }"));
        assert!(bad("{ path: nope.x, op: exists }"));
        assert!(bad("all: []"));
    }

    #[test]
    fn test_nodes_paths_and_skips() {
        let c = cond("all: [ { path: raw.payload.notional, op: lt, value: 5 }, { any: [ { path: raw.payload.desk, op: eq, value: rates }, { path: raw.x, op: exists } ] }, { not: { path: raw.y, op: exists } } ]");
        let r = c.test_nodes(&trade());
        let got: Vec<(&str, &str)> = r.iter().map(|n| (n.node.as_str(), n.result)).collect();
        assert_eq!(
            got,
            vec![("", "no_match"), ("0", "no_match"), ("1", "skipped"), ("1.0", "skipped"), ("1.1", "skipped"), ("2", "skipped"), ("2.0", "skipped")]
        );
        assert_eq!(r[1].actual, Some(json!(25000000)));
        let c = cond("any: [ { path: raw.payload.desk, op: eq, value: rates }, { path: raw.x, op: exists } ]");
        let got: Vec<&str> = c.test_nodes(&trade()).iter().map(|n| n.result).collect();
        assert_eq!(got, vec!["match", "match", "skipped"]);
        let c = cond("not: { path: raw.payload.desk, op: eq, value: fx }");
        let got: Vec<(String, &str)> = c.test_nodes(&trade()).into_iter().map(|n| (n.node, n.result)).collect();
        assert_eq!(got, vec![("".into(), "match"), ("0".into(), "no_match")]);
    }

    #[test]
    fn explain_reports_each_node() {
        let c = cond("all: [ { path: raw.payload.desk, op: eq, value: rates }, { path: raw.payload.notional, op: lt, value: 5 } ]");
        let r = c.explain(&trade());
        assert_eq!(r.len(), 3);
        assert!(!r[0].result);
        assert!(r[1].result);
        assert!(!r[2].result);
        assert_eq!(r[2].actual, Some(json!(25000000)));
    }
}
