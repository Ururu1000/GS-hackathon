//! Alert presentation templates (minijinja).
//!
//! Templates see `raw`, `fields`, `schema`, `source` and `id`. Missing values
//! render as empty strings rather than failing, so a sparse event still makes
//! a readable alert; render errors fall back to the template source.

use minijinja::{Environment, UndefinedBehavior, Value as JValue};
use serde::Serialize;

use crate::event::Event;

pub const MAX_TEMPLATE_LEN: usize = 4096;
pub const MAX_RENDERED_LEN: usize = 8192;

pub fn new_env() -> Environment<'static> {
    let mut env = Environment::new();
    env.set_undefined_behavior(UndefinedBehavior::Chainable);
    env.set_fuel(Some(20_000));
    env.add_filter("money", money);
    env
}

/// `{{ 25000000 | money }}` → `25,000,000`; keeps up to 2 decimals.
fn money(v: JValue) -> String {
    let n: f64 = if let Some(i) = v.as_i64() {
        i as f64
    } else if let Ok(f) = f64::try_from(v.clone()) {
        f
    } else if let Some(s) = v.as_str() {
        match s.trim().parse::<f64>() {
            Ok(f) => f,
            Err(_) => return s.to_string(),
        }
    } else {
        return v.to_string();
    };
    let neg = n < 0.0;
    // Round to cents first so 1000.999 carries into 1,001 instead of `.100`.
    let cents = (n.abs() * 100.0).round() as u128;
    let whole = cents / 100;
    let frac = (cents % 100) as u32;
    let digits = whole.to_string();
    let mut out = String::new();
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    if frac > 0 {
        out.push_str(&format!(".{frac:02}"));
    }
    if neg { format!("-{out}") } else { out }
}

#[derive(Serialize)]
struct Ctx<'a> {
    raw: &'a serde_json::Value,
    fields: &'a serde_json::Map<String, serde_json::Value>,
    schema: &'a str,
    source: &'a str,
    id: &'a str,
}

pub fn context(event: &Event) -> JValue {
    JValue::from_serialize(Ctx {
        raw: &event.raw,
        fields: &event.fields,
        schema: &event.schema,
        source: &event.source,
        id: &event.id,
    })
}

/// Render a named template that was added to `env`; on error return the
/// template name's source text so the alert is still delivered.
pub fn render(env: &Environment<'static>, name: &str, ctx: &JValue) -> String {
    match env.get_template(name).and_then(|t| t.render(ctx)) {
        Ok(mut s) => {
            if s.len() > MAX_RENDERED_LEN {
                let mut cut = MAX_RENDERED_LEN;
                while !s.is_char_boundary(cut) {
                    cut -= 1;
                }
                s.truncate(cut);
                s.push('…');
            }
            s
        }
        Err(e) => {
            tracing::warn!(template = name, error = %e, "template render failed");
            env.get_template(name)
                .map(|t| t.source().to_string())
                .unwrap_or_default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn money_filter() {
        assert_eq!(money(JValue::from(25_000_000)), "25,000,000");
        assert_eq!(money(JValue::from(999)), "999");
        assert_eq!(money(JValue::from(1234.5)), "1,234.50");
        assert_eq!(money(JValue::from("1000")), "1,000");
        assert_eq!(money(JValue::from(1000.999)), "1,001");
        assert_eq!(money(JValue::from(-1234567.5)), "-1,234,567.50");
    }

    #[test]
    fn renders_and_tolerates_missing() {
        let mut env = new_env();
        env.add_template_owned(
            "t",
            "Rejected {{ raw.payload.currency }} {{ raw.payload.notional | money }} trade {{ raw.payload.tradeId }}{{ raw.nope.deeper }}"
                .to_string(),
        )
        .unwrap();
        let e = Event::new(
            "e".into(),
            "s".into(),
            json!({"payload": {"currency": "EUR", "notional": 25000000, "tradeId": "T-1"}}),
        );
        assert_eq!(render(&env, "t", &context(&e)), "Rejected EUR 25,000,000 trade T-1");
    }
}
