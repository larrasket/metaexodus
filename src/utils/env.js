import { existsSync, writeFileSync } from "fs";
import { join } from "path";

const REQUIRED_ENV_VARS = [
  "DB_LOCAL_HOST",
  "DB_LOCAL_PORT",
  "DB_LOCAL_NAME",
  "DB_LOCAL_USERNAME",
  "DB_LOCAL_PASSWORD",
  "METABASE_BASE_URL",
  "METABASE_DATABASE_ID",
  "DB_REMOTE_USERNAME",
  "DB_REMOTE_PASSWORD",
];

const OPTIONAL_ENV_VARS = {
  DB_LOCAL_SSL: "false",
  DB_CONNECTION_TIMEOUT: "30000",
  DB_BATCH_SIZE: "1000",
  SYNC_LOG_LEVEL: "info",
};

function validateRequiredEnvVars() {
  const missing = [];
  const invalid = [];

  for (const envVar of REQUIRED_ENV_VARS) {
    const value = process.env[envVar];

    if (value === undefined || value === null) {
      missing.push(envVar);
    } else if (typeof value !== "string" || value.trim() === "") {
      invalid.push(envVar);
    }
  }

  return {
    success: missing.length === 0 && invalid.length === 0,
    missing,
    invalid,
    total: REQUIRED_ENV_VARS.length,
  };
}

function validateEnvVarFormats() {
  const errors = [];

  const localPort = process.env.DB_LOCAL_PORT;

  if (
    localPort &&
    (isNaN(localPort) ||
      parseInt(localPort) <= 0 ||
      parseInt(localPort) > 65535)
  ) {
    errors.push("DB_LOCAL_PORT must be a valid port number (1-65535)");
  }

  const timeout = process.env.DB_CONNECTION_TIMEOUT;
  if (timeout && (isNaN(timeout) || parseInt(timeout) < 1000)) {
    errors.push(
      "DB_CONNECTION_TIMEOUT must be a number >= 1000 (milliseconds)"
    );
  }

  const batchSize = process.env.DB_BATCH_SIZE;
  if (batchSize && (isNaN(batchSize) || parseInt(batchSize) < 1)) {
    errors.push("DB_BATCH_SIZE must be a positive number");
  }

  const logLevel = process.env.SYNC_LOG_LEVEL;
  const validLogLevels = ["error", "warn", "info", "debug"];
  if (logLevel && !validLogLevels.includes(logLevel)) {
    errors.push("SYNC_LOG_LEVEL must be one of: error, warn, info, debug");
  }

  return {
    success: errors.length === 0,
    errors,
  };
}

function setDefaultEnvVars() {
  for (const [key, defaultValue] of Object.entries(OPTIONAL_ENV_VARS)) {
    if (!process.env[key]) {
      process.env[key] = defaultValue;
    }
  }
}

function createEnvTemplate() {
  const templatePath = join(process.cwd(), ".env.template");

  if (existsSync(templatePath)) {
    return { created: false, path: templatePath };
  }

  const templateContent = `# Local Database Configuration
DB_LOCAL_HOST=localhost
DB_LOCAL_PORT=5432
DB_LOCAL_NAME=your_local_database
DB_LOCAL_USERNAME=your_local_username
DB_LOCAL_PASSWORD=your_local_password

# Metabase API Configuration
METABASE_BASE_URL=https://your-metabase-instance.com
METABASE_DATABASE_ID=1
DB_REMOTE_USERNAME=your_metabase_username
# Note: Passwords with special characters should be quoted: DB_REMOTE_PASSWORD="your_password_with_special_chars"
DB_REMOTE_PASSWORD=your_metabase_password

# Optional Configuration
DB_LOCAL_SSL=false
DB_CONNECTION_TIMEOUT=30000
DB_BATCH_SIZE=1000
SYNC_LOG_LEVEL=info`;

  try {
    writeFileSync(templatePath, templateContent);
    return { created: true, path: templatePath };
  } catch (error) {
    return { created: false, error: error.message, path: templatePath };
  }
}

function validateEnvironment() {
  setDefaultEnvVars();

  const requiredValidation = validateRequiredEnvVars();

  const formatValidation = requiredValidation.success
    ? validateEnvVarFormats()
    : { success: true, errors: [] };

  return {
    success: requiredValidation.success && formatValidation.success,
    required: requiredValidation,
    format: formatValidation,
    allErrors: [
      ...requiredValidation.missing.map(
        (v) => `Missing required variable: ${v}`
      ),
      ...requiredValidation.invalid.map(
        (v) => `Invalid value for variable: ${v}`
      ),
      ...formatValidation.errors,
    ],
  };
}

export {
  validateRequiredEnvVars,
  validateEnvVarFormats,
  validateEnvironment,
  setDefaultEnvVars,
  createEnvTemplate,
  REQUIRED_ENV_VARS,
  OPTIONAL_ENV_VARS,
};
