use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::Resource;

/// Attach semantic labels to every resource whose id matches `match`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LabelRule {
    /// Resource id pattern; `*` matches any run of characters.
    #[serde(rename = "match")]
    pub pattern: String,
    pub set: BTreeMap<String, String>,
}

impl LabelRule {
    pub fn apply(&self, resource: &mut Resource) {
        if glob_match(&self.pattern, &resource.id) {
            for (k, v) in &self.set {
                resource.labels.insert(k.clone(), v.clone());
            }
        }
    }
}

/// Match `text` against `pattern`, where `*` matches any (possibly empty) run.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let (p, t): (Vec<char>, Vec<char>) = (pattern.chars().collect(), text.chars().collect());
    let (mut pi, mut ti) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        if pi < p.len() && p[pi] == '*' {
            star = Some((pi, ti));
            pi += 1;
        } else if pi < p.len() && p[pi] == t[ti] {
            pi += 1;
            ti += 1;
        } else if let Some((sp, st)) = star {
            pi = sp + 1;
            ti = st + 1;
            star = Some((sp, st + 1));
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|&c| c == '*')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ResourceKind;

    #[test]
    fn glob() {
        assert!(glob_match("unit:*:sshd.service", "unit:web1:sshd.service"));
        assert!(glob_match("*", ""));
        assert!(glob_match("disk:*:/", "disk:web1:/"));
        assert!(!glob_match("disk:*:/", "disk:web1:/home"));
        assert!(glob_match("a*b*c", "aXXbYYc"));
        assert!(!glob_match("a*b*c", "aXXbYY"));
    }

    #[test]
    fn rule_applies_labels() {
        let rule = LabelRule {
            pattern: "unit:*:sshd.service".into(),
            set: [("criticality".to_string(), "high".to_string())].into(),
        };
        let mut r = Resource::new("unit:h:sshd.service", ResourceKind::Service, "sshd.service");
        rule.apply(&mut r);
        assert_eq!(r.labels["criticality"], "high");
    }
}
