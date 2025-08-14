import {
  configManager,
  DatabaseConfig,
  DatabaseConfigManager
} from '../../src/config/database.js';

describe('Database Configuration', () => {
  let originalEnv;

  beforeEach(() => {
    originalEnv = { ...process.env };

    process.env.DB_LOCAL_HOST = 'localhost';
    process.env.DB_LOCAL_PORT = '5432';
    process.env.DB_LOCAL_NAME = 'test_local';
    process.env.DB_LOCAL_USERNAME = 'local_user';
    process.env.DB_LOCAL_PASSWORD = 'local_pass';
    process.env.METABASE_BASE_URL = 'https://test-metabase.com';
    process.env.METABASE_DATABASE_ID = '1';
    process.env.DB_REMOTE_USERNAME = 'remote_user';
    process.env.DB_REMOTE_PASSWORD = 'remote_pass';
    process.env.DB_LOCAL_SSL = 'true';
    process.env.DB_CONNECTION_TIMEOUT = '30000';
    process.env.DB_BATCH_SIZE = '1000';
    process.env.SYNC_LOG_LEVEL = 'info';
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  describe('DatabaseConfig', () => {
    test('should create a valid database configuration', () => {
      const config = new DatabaseConfig(
        'localhost',
        '5432',
        'testdb',
        'user',
        'pass',
        true
      );

      expect(config.host).toBe('localhost');
      expect(config.port).toBe(5432);
      expect(config.database).toBe('testdb');
      expect(config.username).toBe('user');
      expect(config.password).toBe('pass');
      expect(config.ssl).toBe(true);
    });

    test('should generate correct connection string', () => {
      const config = new DatabaseConfig(
        'localhost',
        '5432',
        'testdb',
        'user',
        'pass',
        true
      );
      const connectionString = config.getConnectionString();

      expect(connectionString).toBe(
        'postgresql://user:pass@localhost:5432/testdb?ssl=true'
      );
    });

    test('should generate connection string without SSL', () => {
      const config = new DatabaseConfig(
        'localhost',
        '5432',
        'testdb',
        'user',
        'pass',
        false
      );
      const connectionString = config.getConnectionString();

      expect(connectionString).toBe(
        'postgresql://user:pass@localhost:5432/testdb'
      );
    });

    test('should generate correct connection options', () => {
      const config = new DatabaseConfig(
        'localhost',
        '5432',
        'testdb',
        'user',
        'pass',
        true
      );
      const options = config.getConnectionOptions();

      expect(options).toEqual({
        host: 'localhost',
        port: 5432,
        database: 'testdb',
        user: 'user',
        password: 'pass',
        ssl: true,
        connectionTimeoutMillis: 30000,
        idleTimeoutMillis: 30000,
        max: 10,
        min: 2
      });
    });

    test('should validate valid configuration', () => {
      const config = new DatabaseConfig(
        'localhost',
        '5432',
        'testdb',
        'user',
        'pass'
      );
      const validation = config.validate();

      expect(validation.valid).toBe(true);
      expect(validation.errors).toHaveLength(0);
    });

    test('should identify invalid host', () => {
      const config = new DatabaseConfig('', '5432', 'testdb', 'user', 'pass');
      const validation = config.validate();

      expect(validation.valid).toBe(false);
      expect(validation.errors).toContain(
        'Host is required and must be a string'
      );
    });

    test('should identify invalid port', () => {
      const config = new DatabaseConfig(
        'localhost',
        'invalid',
        'testdb',
        'user',
        'pass'
      );
      const validation = config.validate();

      expect(validation.valid).toBe(false);
      expect(validation.errors).toContain(
        'Port must be a valid number between 1 and 65535'
      );
    });

    test('should identify missing database name', () => {
      const config = new DatabaseConfig(
        'localhost',
        '5432',
        '',
        'user',
        'pass'
      );
      const validation = config.validate();

      expect(validation.valid).toBe(false);
      expect(validation.errors).toContain(
        'Database name is required and must be a string'
      );
    });

    test('should identify missing username', () => {
      const config = new DatabaseConfig(
        'localhost',
        '5432',
        'testdb',
        '',
        'pass'
      );
      const validation = config.validate();

      expect(validation.valid).toBe(false);
      expect(validation.errors).toContain(
        'Username is required and must be a string'
      );
    });

    test('should identify missing password', () => {
      const config = new DatabaseConfig(
        'localhost',
        '5432',
        'testdb',
        'user',
        ''
      );
      const validation = config.validate();

      expect(validation.valid).toBe(false);
      expect(validation.errors).toContain(
        'Password is required and must be a string'
      );
    });

    test('should return masked configuration', () => {
      const config = new DatabaseConfig(
        'localhost',
        '5432',
        'testdb',
        'user',
        'secret'
      );
      const masked = config.getMaskedConfig();

      expect(masked.password).toBe('***masked***');
      expect(masked.username).toBe('user');
      expect(masked.host).toBe('localhost');
    });

    test('should create connection string with URL encoded credentials', () => {
      const config = new DatabaseConfig(
        'localhost',
        5432,
        'test_db',
        'user@domain',
        'pass@word:123',
        false
      );

      const connectionString = config.getConnectionString();

      // Should URL encode special characters in username and password
      expect(connectionString).toBe(
        'postgresql://user%40domain:pass%40word%3A123@localhost:5432/test_db'
      );
    });

    test('should handle passwords with various special characters', () => {
      const specialPassword = 'p@ssw0rd!@#$%^&*()_+-=[]{}|;:,.<>?';
      const config = new DatabaseConfig(
        'localhost',
        5432,
        'test_db',
        'test_user',
        specialPassword,
        false
      );

      const connectionString = config.getConnectionString();

      // Should properly encode all special characters
      expect(connectionString).toContain('test_user:');
      expect(connectionString).toContain('@localhost:5432/test_db');
      // The password part should be URL encoded
      expect(connectionString).not.toContain(specialPassword);
    });
  });

  describe('DatabaseConfigManager', () => {
    test('should initialize successfully with valid environment', () => {
      const manager = new DatabaseConfigManager();
      const result = manager.initialize();

      expect(result.success).toBe(true);
      expect(result.message).toBe(
        'Database configurations initialized successfully'
      );
      expect(manager.isReady()).toBe(true);
    });

    test('should fail initialization with missing environment variables', () => {
      delete process.env.DB_LOCAL_HOST;
      delete process.env.DB_REMOTE_HOST;

      const manager = new DatabaseConfigManager();
      const result = manager.initialize();

      expect(result.success).toBe(false);
      expect(result.error).toBe('Environment validation failed');
      expect(result.details).toContain(
        'Missing required variable: DB_LOCAL_HOST'
      );
    });

    test('should return local configuration after initialization', () => {
      const manager = new DatabaseConfigManager();
      manager.initialize();

      const localConfig = manager.getLocalConfig();

      expect(localConfig.host).toBe('localhost');
      expect(localConfig.database).toBe('test_local');
      expect(localConfig.username).toBe('local_user');
    });

    test('should return null for remote configuration (using Metabase API)', () => {
      const manager = new DatabaseConfigManager();
      manager.initialize();

      const remoteConfig = manager.getRemoteConfig();

      expect(remoteConfig).toBeNull();
    });

    test('should throw error when accessing config before initialization', () => {
      const manager = new DatabaseConfigManager();

      expect(() => manager.getLocalConfig()).toThrow(
        'Configuration manager not initialized'
      );
      expect(() => manager.getRemoteConfig()).toThrow(
        'Configuration manager not initialized'
      );
    });

    test('should validate configurations correctly', () => {
      const manager = new DatabaseConfigManager();
      manager.initialize();

      const validation = manager.validateConfig();

      expect(validation.valid).toBe(true);
      expect(validation.errors).toHaveLength(0);
    });

    test('should return validation errors for uninitialized manager', () => {
      const manager = new DatabaseConfigManager();

      const validation = manager.validateConfig();

      expect(validation.valid).toBe(false);
      expect(validation.errors).toContain(
        'Local configuration not initialized'
      );
    });

    test('should return masked configurations', () => {
      const manager = new DatabaseConfigManager();
      manager.initialize();

      const masked = manager.getMaskedConfigs();

      expect(masked.local.password).toBe('***masked***');
      expect(masked.remote).toBeNull();
      expect(masked.local.username).toBe('local_user');
    });

    test('should handle SSL configuration correctly', () => {
      const manager = new DatabaseConfigManager();
      manager.initialize();

      const localConfig = manager.getLocalConfig();
      const remoteConfig = manager.getRemoteConfig();

      expect(localConfig.ssl).toBe(true);
      expect(remoteConfig).toBeNull();
    });

    test('should handle SSL false configuration', () => {
      process.env.DB_LOCAL_SSL = 'false';

      const manager = new DatabaseConfigManager();
      manager.initialize();

      const localConfig = manager.getLocalConfig();

      expect(localConfig.ssl).toBe(false);
    });

    test('should not be ready before initialization', () => {
      const manager = new DatabaseConfigManager();

      expect(manager.isReady()).toBe(false);
    });
  });

  describe('configManager singleton', () => {
    test('should be an instance of DatabaseConfigManager', () => {
      expect(configManager).toBeInstanceOf(DatabaseConfigManager);
    });

    test('should maintain state across imports', () => {
      const result = configManager.initialize();

      expect(result.success).toBe(true);
      expect(configManager.isReady()).toBe(true);
    });
  });
});
