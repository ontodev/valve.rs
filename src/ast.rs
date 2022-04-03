use std::fmt::Debug;

#[derive(Debug, Clone)]
pub enum Expression {
    Null,
    Label(String),
    Field(String, String),
    NamedArg(String, String),
    RegexMatch(String, String),
    RegexSub(String, String, String),
    Function(String, Vec<Box<Expression>>),
}