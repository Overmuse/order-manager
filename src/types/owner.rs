use serde::{Deserialize, Serialize};
use std::fmt::{Display, Error, Formatter};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Owner {
    House,
    Strategy(String, Option<String>),
}

impl Display for Owner {
    fn fmt(&self, formatter: &mut Formatter) -> std::result::Result<(), Error> {
        match self {
            Owner::House => formatter.write_str("House"),
            Owner::Strategy(strategy, sub_strategy) => {
                if let Some(sub_strategy) = sub_strategy {
                    formatter.write_str(strategy)?;
                    formatter.write_str(":")?;
                    formatter.write_str(sub_strategy)
                } else {
                    formatter.write_str(strategy)
                }
            }
        }
    }
}
