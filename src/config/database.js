import { createEnvTemplate, validateEnvironment } from "../utils/env.js";

class DatabaseConfig {
  constructor(host, port, database, username, password, ssl = false) {
    this.host = host;
    this.port = parseInt(port);
    this.database = database;
    this.username = username;
    this.password = password;
    this.ssl = ssl;
  }

  getConnectionString() {
    const sslParam = this.ssl ? "?ssl=true" : "";
    // URL encode the username and password to handle special characters
    const encodedUsername = encodeURIComponent(this.username);
    const encodedPassword = encodeURIComponent(this.password);
    return `postgresql://${encodedUsername}:${encodedPassword}@${this.host}:${this.port}/${this.database}${sslParam}`;
  }

  getConnectionOptions() {
    return {
      host: this.host,
      port: this.port,
      database: this.database,
      user: this.username,
      password: this.password,
      ssl: this.ssl,
      connectionTimeoutMillis:
        parseInt(process.env.DB_CONNECTION_TIMEOUT) || 30000,
      idleTimeoutMillis: 30000,
      max: 10,
      min: 2,
    };
  }

  validate() {
    const errors = [];

    if (!this.host || typeof this.host !== "string") {
      errors.push("Host is required and must be a string");
    }

    if (
      !this.port ||
      Number.isNaN(this.port) ||
      this.port <= 0 ||
      this.port > 65535
    ) {
      errors.push("Port must be a valid number between 1 and 65535");
    }

    if (!this.database || typeof this.database !== "string") {
      errors.push("Database name is required and must be a string");
    }

    if (!this.username || typeof this.username !== "string") {
      errors.push("Username is required and must be a string");
    }

    if (!this.password || typeof this.password !== "string") {
      errors.push("Password is required and must be a string");
    }

    return {
      valid: errors.length === 0,
      errors,
    };
  }

  getMaskedConfig() {
    return {
      host: this.host,
      port: this.port,
      database: this.database,
      username: this.username,
      password: "***masked***",
      ssl: this.ssl,
    };
  }
}

class DatabaseConfigManager {
  constructor() {
    this.localConfig = null;
    this.remoteConfig = null;
    this.validated = false;
  }

  initialize() {
    const envValidation = validateEnvironment();

    if (!envValidation.success) {
      return {
        success: false,
        error: "Environment validation failed",
        details: envValidation.allErrors,
      };
    }

    try {
      this.localConfig = new DatabaseConfig(
        process.env.DB_LOCAL_HOST,
        process.env.DB_LOCAL_PORT,
        process.env.DB_LOCAL_NAME,
        process.env.DB_LOCAL_USERNAME,
        process.env.DB_LOCAL_PASSWORD,
        process.env.DB_LOCAL_SSL === "true"
      );

      this.remoteConfig = null;

      const localValidation = this.localConfig.validate();

      if (!localValidation.valid) {
        return {
          success: false,
          error: "Database configuration validation failed",
          details: localValidation.errors.map((e) => `Local DB: ${e}`),
        };
      }

      this.validated = true;
      return {
        success: true,
        message: "Database configurations initialized successfully",
      };
    } catch (error) {
      return {
        success: false,
        error: "Failed to initialize database configurations",
        details: [error.message],
      };
    }
  }

  getLocalConfig() {
    if (!this.validated) {
      throw new Error(
        "Configuration manager not initialized. Call initialize() first."
      );
    }
    return this.localConfig;
  }

  getRemoteConfig() {
    if (!this.validated) {
      throw new Error(
        "Configuration manager not initialized. Call initialize() first."
      );
    }
    return this.remoteConfig;
  }

  validateConfig() {
    if (!this.localConfig) {
      return {
        valid: false,
        errors: ["Local configuration not initialized"],
      };
    }

    const localValidation = this.localConfig.validate();

    return {
      valid: localValidation.valid,
      errors: localValidation.errors.map((e) => `Local DB: ${e}`),
    };
  }

  createEnvTemplate() {
    return createEnvTemplate();
  }

  getMaskedConfigs() {
    return {
      local: this.localConfig ? this.localConfig.getMaskedConfig() : null,
      remote: this.remoteConfig ? this.remoteConfig.getMaskedConfig() : null,
    };
  }

  isReady() {
    return Boolean(this.validated && this.localConfig);
  }
}

const configManager = new DatabaseConfigManager();

export { DatabaseConfig, DatabaseConfigManager, configManager };
