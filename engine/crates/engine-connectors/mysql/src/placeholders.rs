//! MySQL's prepared statements take `?` placeholders in order; the engine plans with `$1`, `$2`, ...
//! which may repeat. Rewrites one into the other and says which parameter each `?` takes.

/// `sql` with every `$n` outside a string literal or quoted identifier replaced by `?`, and the
/// zero-based parameter index each `?` binds, in order. Backslashes are not escapes: the session
/// runs with `NO_BACKSLASH_ESCAPES`, so a quote inside a literal is doubled.
pub fn to_question_marks(sql: &str) -> (String, Vec<usize>) {
    let mut out = String::with_capacity(sql.len());
    let mut order = Vec::new();
    let mut chars = sql.chars().peekable();
    let mut quote: Option<char> = None;
    while let Some(c) = chars.next() {
        match quote {
            Some(q) => {
                out.push(c);
                if c == q {
                    // a doubled quote stays inside the literal
                    if chars.peek() == Some(&q) {
                        out.push(chars.next().unwrap());
                    } else {
                        quote = None;
                    }
                }
            }
            None => match c {
                '\'' | '"' | '`' => {
                    quote = Some(c);
                    out.push(c);
                }
                '$' if chars.peek().is_some_and(|d| d.is_ascii_digit()) => {
                    let mut n: usize = 0;
                    while let Some(d) = chars.peek().and_then(|d| d.to_digit(10)) {
                        n = n * 10 + d as usize;
                        chars.next();
                    }
                    out.push('?');
                    order.push(n.saturating_sub(1));
                }
                _ => out.push(c),
            },
        }
    }
    (out, order)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_numbered_placeholders_in_order_including_repeats() {
        let (sql, order) = to_question_marks("SELECT $2, $1 FROM t WHERE a = $1 AND b = $12");
        assert_eq!(sql, "SELECT ?, ? FROM t WHERE a = ? AND b = ?");
        assert_eq!(order, vec![1, 0, 0, 11]);
    }

    #[test]
    fn leaves_literals_and_identifiers_alone() {
        let (sql, order) = to_question_marks(
            r#"SELECT '$1', 'it''s $2', `col$3`, "x$4" FROM t WHERE a = $1 AND b = '\' AND c = $2"#,
        );
        assert_eq!(
            sql,
            r#"SELECT '$1', 'it''s $2', `col$3`, "x$4" FROM t WHERE a = ? AND b = '\' AND c = ?"#
        );
        assert_eq!(order, vec![0, 1]);
    }

    #[test]
    fn a_dollar_that_is_not_a_placeholder_passes_through() {
        let (sql, order) = to_question_marks("SELECT `a$b`, a$b, '$' FROM t");
        assert_eq!(sql, "SELECT `a$b`, a$b, '$' FROM t");
        assert!(order.is_empty());
    }
}
