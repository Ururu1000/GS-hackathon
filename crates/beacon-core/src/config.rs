//! Config snapshot: schema mappings + rules + templates + indexes, compiled
//! together and swapped atomically by the caller (M0 loads `config/` at
//! startup; M2 adds the Postgres-backed versioned store and hot reload).

use std::collections::HashSet;
use std::path::Path as FsPath;

use anyhow::{Context, Result, bail};
use minijinja::Environment;

use crate::mapping::{SchemaMapping, SchemaMappingSpec};
use crate::matcher::RuleIndex;
use crate::rule::{CompiledRule, RuleError, RuleSpec};
use crate::template::{self, MAX_TEMPLATE_LEN};

pub struct ConfigSnapshot {
    pub revision: i64,
    /// Sorted by priority, highest first.
    pub mappings: Vec<SchemaMapping>,
    pub rules: Vec<CompiledRule>,
    pub env: Environment<'static>,
    pub(crate) fire_index: RuleIndex,
    pub(crate) recover_index: RuleIndex,
}

impl std::fmt::Debug for ConfigSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigSnapshot")
            .field("revision", &self.revision)
            .field("mappings", &self.mappings.len())
            .field("rules", &self.rules.len())
            .finish()
    }
}

impl ConfigSnapshot {
    /// Compile a full snapshot. Every problem is reported, not just the first.
    pub fn build(revision: i64, schemas: Vec<SchemaMappingSpec>, rules: Vec<RuleSpec>) -> Result<ConfigSnapshot> {
        Self::build_with(revision, schemas, rules.into_iter().map(|r| (r, revision)).collect())
    }

    /// Like [`build`](Self::build), with a revision per rule (rules stored in Postgres).
    pub fn build_with(revision: i64, schemas: Vec<SchemaMappingSpec>, rules: Vec<(RuleSpec, i64)>) -> Result<ConfigSnapshot> {
        let mut errors: Vec<String> = Vec::new();

        let mut seen = HashSet::new();
        let mut mappings = Vec::new();
        for s in &schemas {
            if !seen.insert(s.schema.clone()) {
                errors.push(format!("duplicate schema `{}`", s.schema));
                continue;
            }
            match SchemaMapping::compile(s) {
                Ok(m) => mappings.push(m),
                Err(e) => errors.push(e.to_string()),
            }
        }
        mappings.sort_by(|a, b| b.priority.cmp(&a.priority).then_with(|| a.schema.cmp(&b.schema)));

        let mut env = template::new_env();
        let mut seen = HashSet::new();
        let mut compiled = Vec::new();
        for (r, rule_revision) in rules {
            if !seen.insert(r.id.clone()) {
                errors.push(format!("duplicate rule id `{}`", r.id));
                continue;
            }
            let rule = match CompiledRule::compile(r, rule_revision) {
                Ok(rule) => rule,
                Err(e) => {
                    errors.push(e.to_string());
                    continue;
                }
            };
            for (which, src) in [("title", rule.title_template()), ("body", rule.body_template())] {
                let res = if src.len() > MAX_TEMPLATE_LEN {
                    Err(format!("longer than {MAX_TEMPLATE_LEN} characters"))
                } else {
                    env.add_template_owned(format!("{}:{which}", rule.id()), src)
                        .map_err(|e| e.to_string())
                };
                if let Err(message) = res {
                    errors.push(
                        RuleError::Template {
                            rule: rule.id().to_string(),
                            which,
                            message,
                        }
                        .to_string(),
                    );
                }
            }
            compiled.push(rule);
        }
        if !errors.is_empty() {
            bail!("invalid config:\n  - {}", errors.join("\n  - "));
        }
        let fire_index = RuleIndex::build(compiled.iter().enumerate().map(|(i, r)| (i, Some(&r.when))));
        let recover_index =
            RuleIndex::build(compiled.iter().enumerate().map(|(i, r)| (i, r.recover_when.as_ref())));
        Ok(ConfigSnapshot {
            revision,
            mappings,
            rules: compiled,
            env,
            fire_index,
            recover_index,
        })
    }
}

/// Load `<dir>/schemas/*.yaml` and `<dir>/rules/*.yaml`.
pub fn load_config_dir(dir: &FsPath) -> Result<ConfigSnapshot> {
    let schemas = load_yaml_files::<SchemaMappingSpec>(&dir.join("schemas"))?;
    let rules = load_yaml_files::<RuleSpec>(&dir.join("rules"))?;
    ConfigSnapshot::build(1, schemas, rules)
}

pub fn load_yaml_files<T: serde::de::DeserializeOwned>(dir: &FsPath) -> Result<Vec<T>> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    let mut paths: Vec<_> = std::fs::read_dir(dir)
        .with_context(|| format!("reading {}", dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| matches!(p.extension().and_then(|e| e.to_str()), Some("yaml" | "yml")))
        .collect();
    paths.sort();
    for p in paths {
        let text = std::fs::read_to_string(&p).with_context(|| format!("reading {}", p.display()))?;
        let v: T = serde_saphyr::from_str(&text).with_context(|| format!("parsing {}", p.display()))?;
        out.push(v);
    }
    Ok(out)
}
