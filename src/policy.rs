#[derive(Default)]
pub struct Policy {
    pub order_prefix: String,
}

impl Policy {
    const SEPARATOR: char = '_';

    pub fn new() -> Self {
        Default::default()
    }

    pub fn order_prefix<S: Into<String>>(&mut self, prefix: S) -> &mut Self {
        self.order_prefix = prefix.into();
        self
    }

    pub fn add_prefix<'a>(&self, id: &'a str) -> String {
        let mut prefix = self.order_prefix.clone();
        prefix.push(Self::SEPARATOR);
        prefix.push_str(id);
        prefix
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn builder() {
        let mut policy = Policy::new();
        policy.order_prefix("TEST");
        assert_eq!(policy.add_prefix("TESTER"), "TEST_TESTER".to_string());
    }
}
