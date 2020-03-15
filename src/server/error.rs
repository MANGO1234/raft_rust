use std::{error, fmt, result};

pub type Result<T> = result::Result<T, SvrErr>;

#[derive(Debug)]
pub enum SvrErr {
    Io(std::io::Error),
    Parse(serde_json::Error),
    UTF8(std::string::FromUtf8Error),
}

impl fmt::Display for SvrErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SvrErr::Io(ref err) => write!(f, "IO error: {}", err),
            SvrErr::Parse(ref err) => write!(f, "Parse error: {}", err),
            SvrErr::UTF8(ref err) => write!(f, "Parse error: {}", err),
        }
    }
}

impl error::Error for SvrErr {
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            SvrErr::Io(ref err) => Some(err),
            SvrErr::Parse(ref err) => Some(err),
            SvrErr::UTF8(ref err) => Some(err),
        }
    }
}

impl From<std::io::Error> for SvrErr {
    fn from(err: std::io::Error) -> SvrErr {
        SvrErr::Io(err)
    }
}

impl From<serde_json::Error> for SvrErr {
    fn from(err: serde_json::Error) -> SvrErr {
        SvrErr::Parse(err)
    }
}

impl From<std::string::FromUtf8Error> for SvrErr {
    fn from(err: std::string::FromUtf8Error) -> SvrErr {
        SvrErr::UTF8(err)
    }
}
