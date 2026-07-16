use crate::errors::ResponseError;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct CliError(pub ResponseError);

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.message)
    }
}

impl Error for CliError {}
