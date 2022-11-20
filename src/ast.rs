use std::fmt;

/// Represents an expression as parsed using [Valve's grammar](../valve_grammar/index.html).
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

// We use Debug instead of Display because it is not possible to display a Vec.
// TODO: There might be some way around this limitation. We should look into it eventually but this
// is low on the priority list because this code is working fine.
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
