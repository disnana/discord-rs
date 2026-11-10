mod cogs;
pub mod worker;

pub type AppError = Box<dyn std::error::Error + Send + Sync>;
