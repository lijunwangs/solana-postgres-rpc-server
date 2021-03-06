use {std::io, thiserror::Error};

/// Errors returned by rpc server
#[derive(Error, Debug)]
pub enum PostgresRpcServerError {
    /// Error opening the configuration file; for example, when the file
    /// is not found or when the validator process has no permission to read it.
    #[error("Error opening config file. Error detail: ({0}).")]
    ConfigFileOpenError(#[from] io::Error),

    /// Error in reading the content of the config file or the content
    /// is not in the expected format.
    #[error("Error reading config file. Error message: ({msg})")]
    ConfigFileReadError { msg: String },

    #[error("Error connecting to the backend data store. Error message: ({msg})")]
    DataStoreConnectionError { msg: String },

    #[error("Error preparing data store schema. Error message: ({msg})")]
    DataSchemaError { msg: String },

    #[error("Error preparing data store schema. Error message: ({msg})")]
    ConfigurationError { msg: String },

    #[error("Error executing the database query. Error message: ({msg})")]
    DatabaseQueryError { msg: String },

    #[error("The object is not found.")]
    ObjectNotFound { msg: String },

    #[error("More than one object is found when at most 1 is expected.")]
    MoreThanOneObjectFound { msg: String },
}

pub type Result<T> = std::result::Result<T, PostgresRpcServerError>;
