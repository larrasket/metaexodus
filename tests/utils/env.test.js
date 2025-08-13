import {
  createEnvTemplate,
  OPTIONAL_ENV_VARS,
  REQUIRED_ENV_VARS,
  setDefaultEnvVars,
  validateEnvironment,
  validateEnvVarFormats,
  validateRequiredEnvVars
} from '../../src/utils/env.js';

describe('Environment Validation Utility', () => {
  let originalEnv;

  beforeEach(() => {
    originalEnv = { ...process.env };

    REQUIRED_ENV_VARS.forEach((key) => {
      delete process.env[key];
    });
    Object.keys(OPTIONAL_ENV_VARS).forEach((key) => {
      delete process.env[key];
    });
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  describe('validateRequiredEnvVars', () => {
    test('should validate all required environment variables', () => {
      // Set all required environment variables
      REQUIRED_ENV_VARS.forEach((key) => {
        process.env[key] = 'test_value';
      });

      const result = validateRequiredEnvVars();
      expect(result.success).toBe(true);
      expect(result.missing).toHaveLength(0);
      expect(result.invalid).toHaveLength(0);
      expect(result.total).toBe(9);
    });

    test('should handle passwords with special characters', () => {
      // Set all required environment variables
      REQUIRED_ENV_VARS.forEach((key) => {
        process.env[key] = 'test_value';
      });

      // Set a password with special characters
      process.env.DB_REMOTE_PASSWORD = 'p@ssw0rd!@#$%^&*()_+-=[]{}|;:,.<>?';

      const result = validateRequiredEnvVars();
      expect(result.success).toBe(true);
      expect(result.invalid).toHaveLength(0);
      expect(result.missing).toHaveLength(0);
    });

    test('should handle quoted passwords with special characters', () => {
      // Set all required environment variables
      REQUIRED_ENV_VARS.forEach((key) => {
        process.env[key] = 'test_value';
      });

      // Set a quoted password with special characters
      process.env.DB_REMOTE_PASSWORD = '"p@ssw0rd!@#$%^&*()_+-=[]{}|;:,.<>?"';

      const result = validateRequiredEnvVars();
      expect(result.success).toBe(true);
      expect(result.invalid).toHaveLength(0);
      expect(result.missing).toHaveLength(0);
    });

    test('should identify missing variables', () => {
      process.env.DB_LOCAL_HOST = 'localhost';
      process.env.DB_LOCAL_PORT = '5432';

      const result = validateRequiredEnvVars();

      expect(result.success).toBe(false);
      expect(result.missing).toContain('DB_LOCAL_NAME');
      expect(result.missing).toContain('DB_LOCAL_USERNAME');
      expect(result.missing.length).toBeGreaterThan(0);
    });

    test('should identify invalid (empty) variables', () => {
      REQUIRED_ENV_VARS.forEach((key) => {
        process.env[key] = key === 'DB_LOCAL_HOST' ? '' : 'test_value';
      });

      const result = validateRequiredEnvVars();

      expect(result.success).toBe(false);
      expect(result.invalid).toContain('DB_LOCAL_HOST');
    });
  });

  describe('validateEnvVarFormats', () => {
    test('should validate port numbers correctly', () => {
      process.env.DB_LOCAL_PORT = '5432';
      process.env.DB_REMOTE_PORT = '5432';

      const result = validateEnvVarFormats();

      expect(result.success).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    test('should reject invalid port numbers', () => {
      process.env.DB_LOCAL_PORT = 'invalid';

      const result = validateEnvVarFormats();

      expect(result.success).toBe(false);
      expect(result.errors).toContain(
        'DB_LOCAL_PORT must be a valid port number (1-65535)'
      );
    });

    test('should validate timeout correctly', () => {
      process.env.DB_CONNECTION_TIMEOUT = '30000';

      const result = validateEnvVarFormats();

      expect(result.success).toBe(true);
    });

    test('should reject invalid timeout', () => {
      process.env.DB_CONNECTION_TIMEOUT = '500';

      const result = validateEnvVarFormats();

      expect(result.success).toBe(false);
      expect(result.errors).toContain(
        'DB_CONNECTION_TIMEOUT must be a number >= 1000 (milliseconds)'
      );
    });

    test('should validate batch size correctly', () => {
      process.env.DB_BATCH_SIZE = '1000';

      const result = validateEnvVarFormats();

      expect(result.success).toBe(true);
    });

    test('should reject invalid batch size', () => {
      process.env.DB_BATCH_SIZE = '0';

      const result = validateEnvVarFormats();

      expect(result.success).toBe(false);
      expect(result.errors).toContain(
        'DB_BATCH_SIZE must be a positive number'
      );
    });

    test('should validate log level correctly', () => {
      process.env.SYNC_LOG_LEVEL = 'info';

      const result = validateEnvVarFormats();

      expect(result.success).toBe(true);
    });

    test('should reject invalid log level', () => {
      process.env.SYNC_LOG_LEVEL = 'invalid';

      const result = validateEnvVarFormats();

      expect(result.success).toBe(false);
      expect(result.errors).toContain(
        'SYNC_LOG_LEVEL must be one of: error, warn, info, debug'
      );
    });
  });

  describe('setDefaultEnvVars', () => {
    test('should set default values for missing optional variables', () => {
      setDefaultEnvVars();

      Object.entries(OPTIONAL_ENV_VARS).forEach(([key, defaultValue]) => {
        expect(process.env[key]).toBe(defaultValue);
      });
    });

    test('should not override existing optional variables', () => {
      process.env.DB_SSL = 'true';
      process.env.SYNC_LOG_LEVEL = 'debug';

      setDefaultEnvVars();

      expect(process.env.DB_SSL).toBe('true');
      expect(process.env.SYNC_LOG_LEVEL).toBe('debug');
    });
  });

  describe('validateEnvironment', () => {
    test('should return comprehensive validation results', () => {
      REQUIRED_ENV_VARS.forEach((key) => {
        process.env[key] = 'test_value';
      });

      process.env.DB_LOCAL_PORT = '5432';
      process.env.DB_REMOTE_PORT = '5432';

      const result = validateEnvironment();

      expect(result.success).toBe(true);
      expect(result.required.success).toBe(true);
      expect(result.format.success).toBe(true);
      expect(result.allErrors).toHaveLength(0);
    });

    test('should collect all errors', () => {
      process.env.DB_LOCAL_HOST = 'localhost';
      process.env.DB_LOCAL_PORT = 'invalid';

      const result = validateEnvironment();

      expect(result.success).toBe(false);
      expect(result.allErrors.length).toBeGreaterThan(0);
      expect(
        result.allErrors.some((error) =>
          error.includes('Missing required variable')
        )
      ).toBe(true);
    });

    test('should set default values during validation', () => {
      REQUIRED_ENV_VARS.forEach((key) => {
        process.env[key] = 'test_value';
      });
      process.env.DB_LOCAL_PORT = '5432';
      process.env.DB_REMOTE_PORT = '5432';

      validateEnvironment();

      expect(process.env.DB_LOCAL_SSL).toBe('false');
      expect(process.env.DB_CONNECTION_TIMEOUT).toBe('30000');
      expect(process.env.DB_BATCH_SIZE).toBe('1000');
      expect(process.env.SYNC_LOG_LEVEL).toBe('info');
    });
  });

  describe('createEnvTemplate', () => {
    test('should indicate when template already exists', () => {
      const result = createEnvTemplate();

      expect(result.created).toBe(false);
      expect(result.path).toContain('.env.template');
    });
  });
});
