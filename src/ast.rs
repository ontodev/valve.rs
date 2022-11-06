use std::fmt;
//use std::fmt::Debug;

/// Represents an expression as parsed using [Valve's grammar](../valve_grammar/index.html).
//#[derive(Debug, Clone)]
#[derive(Clone)]
pub enum Expression {
    None,
    Null,
    NotNull,
    Label(String),
    Field(String, String),
    NamedArg(String, String),
    RegexMatch(String, String),
    RegexSub(String, String, String),
    Function(String, Vec<Box<Expression>>),
}

// TODO: It would be better to implement this as std::fmt::Display instead of Debug.
impl std::fmt::Debug for Expression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expression::None => write!(f, ""),
            Expression::Null => write!(f, "\"null\""),
            Expression::NotNull => write!(f, "\"not null\""),
            Expression::Label(l) => write!(f, "{{\"label\": \"{}\"}}", l),
            Expression::Field(a, b) => write!(f, "{{\"field\": [{:?}, {:?}]}}", a, b),
            Expression::NamedArg(a, b) => write!(f, "{{\"named_arg\": [{:?}, {:?}]}}", a, b),
            Expression::RegexMatch(pattern, flags) => write!(
                f,
                "{{\"match\": {{\"pattern\": \"{}\", \"flags\": \"{}\"}}}}",
                pattern, flags
            ),
            Expression::RegexSub(pattern, replace, flags) => write!(
                f,
                "{{\"match\": {{\"pattern\": \"{}\", \"replace\": \"{}\", \"flags\": \"{}\"}}}}",
                pattern, replace, flags
            ),
            Expression::Function(name, args) => {
                write!(f, "{{\"function\": {{\"name\": \"{}\", \"args\": {:?}}}}}", name, args)
            }
        }
    }
}
