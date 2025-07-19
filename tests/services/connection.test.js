import { jest } from '@jest/globals';

jest.unstable_mockModule('pg', () => {
  const mockClient = {
    connect: jest.fn().mockResolvedValue(),
    query: jest.fn().mockResolvedValue({ rows: [] }),
    end: jest.fn().mockResolvedValue(),
    release: jest.fn()
  };

  const mockPool = {
    connect: jest.fn().mockResolvedValue({
      query: jest.fn().mockResolvedValue({ rows: [] }),
      release: jest.fn()
    }),
    end: jest.fn().mockResolvedValue(),
    totalCount: 5,
    idleCount: 3,
    waitingCount: 0
  };

  return {
    Client: jest.fn().mockImplementation(() => mockClient),
    Pool: jest.fn().mockImplementation(() => mockPool)
  };
});

const { ConnectionService, connectionService } = await import('../../src/services/connection.js');
const { configManager } = await import('../../src/config/database.js');

const { Client, Pool } = await import('pg');
const mockClient = new Client();
const mockPool = new Pool();

describe('Connection Service', () => {
  let originalEnv;

  beforeEach(() => {
    originalEnv = { ...process.env };

    process.env.DB_LOCAL_HOST = 'localhost';
    process.env.DB_LOCAL_PORT = '5432';
    process.env.DB_LOCAL_NAME = 'test_db';
    process.env.DB_LOCAL_USERNAME = 'test_user';
    process.env.DB_LOCAL_PASSWORD = 'test_pass';
    process.env.DB_LOCAL_SSL = 'false';
    process.env.DB_CONNECTION_TIMEOUT = '30000';
    process.env.SYNC_LOG_LEVEL = 'info';
    
    process.env.METABASE_BASE_URL = 'https://test-metabase.com';
    process.env.METABASE_DATABASE_ID = '1';
    process.env.DB_REMOTE_USERNAME = 'remote_user';
    process.env.DB_REMOTE_PASSWORD = 'remote_pass';
    process.env.DB_BATCH_SIZE = '1000';

    jest.clearAllMocks();

    mockClient.connect.mockResolvedValue();
    mockClient.query.mockResolvedValue({ rows: [] });
    mockClient.end.mockResolvedValue();
    mockPool.connect.mockResolvedValue({
      query: jest.fn().mockResolvedValue({ rows: [] }),
      release: jest.fn()
    });
    mockPool.end.mockResolvedValue();
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  describe('ConnectionService', () => {
    test('should initialize successfully', async () => {
      const service = new ConnectionService();

      const result = await service.initialize();

      expect(result.success).toBe(true);
      expect(result.message).toBe('Connection service initialized successfully');
    });

    test('should fail initialization with invalid config', async () => {
      delete process.env.DB_LOCAL_HOST;
      
      configManager.validated = false;
      configManager.localConfig = null;

      const service = new ConnectionService();
      const result = await service.initialize();

      expect(result.success).toBe(false);
      expect(result.error).toBe('Failed to initialize database configurations');
    });

    test('should connect to local database', async () => {
      const service = new ConnectionService();
      await service.initialize();

      const client = await service.connectLocal();

      expect(client).toBeDefined();
      expect(mockClient.connect).toHaveBeenCalled();
    });

    test('should only support local database connections', () => {
      const service = new ConnectionService();

      expect(service.remotePool).toBeNull();
      expect(service.remoteClient).toBeNull();
    });

    test('should throw error when connecting without initialization', async () => {
      const service = new ConnectionService();

      await expect(service.connectLocal()).rejects.toThrow('Connection service not initialized. Call initialize() first.');
    });

    test('should handle connection errors gracefully', async () => {
      const service = new ConnectionService();
      await service.initialize();

      mockClient.connect.mockRejectedValue(new Error('Connection failed'));
      jest.spyOn(service, 'sleep').mockResolvedValue();

      await expect(service.connectLocal()).rejects.toThrow('Failed to connect to local database after 3 attempts: Connection failed');
    });

    test('should create local connection pool', async () => {
      const service = new ConnectionService();
      await service.initialize();

      const pool = await service.createLocalPool();

      expect(pool).toBeDefined();
      expect(mockPool.connect).toHaveBeenCalled();
    });

    test('should not have remote connection pool functionality', () => {
      const service = new ConnectionService();
      
      expect(service.remotePool).toBeNull();
      expect(service.remoteClient).toBeNull();
    });

    test('should test connections successfully', async () => {
      const service = new ConnectionService();
      await service.initialize();

      const results = await service.testConnections();

      expect(results.local.success).toBe(true);
      expect(results.overall).toBe(true);
    });

    test('should handle connection test failures', async () => {
      const service = new ConnectionService();
      await service.initialize();

      mockClient.connect.mockRejectedValue(new Error('Connection failed'));

      const results = await service.testConnections();

      expect(results.local.success).toBe(false);
      expect(results.overall).toBe(false);
      expect(results.local.error).toContain('Connection failed');
    });

    test('should check connection health for client', async () => {
      const service = new ConnectionService();
      await service.initialize();

      const health = await service.checkConnectionHealth(mockClient);

      expect(health.healthy).toBe(true);
      expect(health.responseTime).toBeGreaterThanOrEqual(0);
    });

    test('should check connection health for pool', async () => {
      const service = new ConnectionService();
      await service.initialize();

      const health = await service.checkConnectionHealth(mockPool);

      expect(health.healthy).toBe(true);
      expect(health.responseTime).toBeGreaterThanOrEqual(0);
    });

    test('should handle health check failures', async () => {
      const service = new ConnectionService();
      await service.initialize();

      const failingClient = {
        query: jest.fn().mockRejectedValue(new Error('Health check failed'))
      };

      const health = await service.checkConnectionHealth(failingClient);

      expect(health.healthy).toBe(false);
      expect(health.error).toBe('Health check failed');
    });

    test('should return connection info when initialized', async () => {
      const service = new ConnectionService();
      await service.initialize();
      await service.createLocalPool();

      const info = service.getConnectionInfo();

      expect(info.initialized).toBe(true);
      expect(info.localPool).toBeDefined();
      expect(info.localPool.totalCount).toBe(5);
      expect(info.localPool.idleCount).toBe(3);
      expect(info.localPool.waitingCount).toBe(0);
    });

    test('should return uninitialized info when not initialized', () => {
      const service = new ConnectionService();

      const info = service.getConnectionInfo();

      expect(info.initialized).toBe(false);
    });

    test('should close all connections successfully', async () => {
      const service = new ConnectionService();
      await service.initialize();
      
      service.localClient = mockClient;
      service.localPool = mockPool;

      await service.closeConnections();

      expect(mockClient.end).toHaveBeenCalled();
      expect(mockPool.end).toHaveBeenCalled();
    });

    test('should handle errors during connection cleanup', async () => {
      const service = new ConnectionService();
      await service.initialize();
      
      service.localClient = mockClient;
      mockClient.end.mockRejectedValue(new Error('Cleanup failed'));

      await expect(service.closeConnections()).rejects.toThrow('Errors during cleanup');
    });

    test('should throw error when creating pool without initialization', async () => {
      const service = new ConnectionService();

      await expect(service.createLocalPool()).rejects.toThrow('Connection service not initialized. Call initialize() first.');
    });

    test('should retry connection with exponential backoff', async () => {
      const service = new ConnectionService();
      await service.initialize();

      let attemptCount = 0;
      mockClient.connect.mockImplementation(() => {
        attemptCount++;
        if (attemptCount < 3) {
          return Promise.reject(new Error('Connection failed'));
        }
        return Promise.resolve();
      });

      jest.spyOn(service, 'sleep').mockResolvedValue();

      const client = await service.connectLocal();

      expect(attemptCount).toBe(3);
      expect(service.sleep).toHaveBeenCalledTimes(2);
      expect(client).toBe(mockClient);
    });

    test('should fail after max retry attempts', async () => {
      const service = new ConnectionService();
      await service.initialize();

      mockClient.connect.mockRejectedValue(new Error('Connection failed'));
      jest.spyOn(service, 'sleep').mockResolvedValue();

      await expect(service.connectLocal()).rejects.toThrow('Failed to connect to local database after 3 attempts');
      expect(service.sleep).toHaveBeenCalledTimes(2);
    });

    test('should reset retry attempts after successful connection', async () => {
      const service = new ConnectionService();
      await service.initialize();

      await service.connectLocal();
      expect(service.connectionAttempts.get('local')).toBe(0);
    });

    test('should configure retry settings', () => {
      const service = new ConnectionService();
      
      service.configureRetry({ maxRetries: 5, baseDelay: 2000 });
      
      expect(service.retryConfig.maxRetries).toBe(5);
      expect(service.retryConfig.baseDelay).toBe(2000);
    });

    test('should reset retry attempts for specific connection type', () => {
      const service = new ConnectionService();
      
      service.connectionAttempts.set('local', 3);
      service.resetRetryAttempts('local');
      
      expect(service.connectionAttempts.get('local')).toBe(0);
    });

    test('should monitor connection pools', async () => {
      const service = new ConnectionService();
      await service.initialize();
      
      service.localPool = mockPool;

      const monitoring = await service.monitorPools();

      expect(monitoring.local.healthy).toBe(true);
      expect(monitoring.local.poolStats).toEqual({
        totalCount: 5,
        idleCount: 3,
        waitingCount: 0
      });
    });

    test('should handle pool monitoring errors', async () => {
      const service = new ConnectionService();
      await service.initialize();
      
      const failingPool = {
        connect: jest.fn().mockRejectedValue(new Error('Pool connection failed'))
      };
      service.localPool = failingPool;

      const monitoring = await service.monitorPools();

      expect(monitoring.local.healthy).toBe(false);
      expect(monitoring.local.error).toBe('Pool connection failed');
    });

    test('should calculate exponential backoff delay correctly', async () => {
      const service = new ConnectionService();
      await service.initialize();

      mockClient.connect.mockRejectedValue(new Error('Connection failed'));
      const sleepSpy = jest.spyOn(service, 'sleep').mockResolvedValue();

      try {
        await service.connectLocal();
      } catch (error) {
      }

      expect(sleepSpy).toHaveBeenCalledWith(1000);
      expect(sleepSpy).toHaveBeenCalledWith(2000);
      expect(sleepSpy).toHaveBeenCalledTimes(2);
    });

    test('should respect max delay in exponential backoff', async () => {
      const service = new ConnectionService();
      service.configureRetry({ maxDelay: 3000 });
      await service.initialize();

      mockClient.connect.mockRejectedValue(new Error('Connection failed'));
      const sleepSpy = jest.spyOn(service, 'sleep').mockResolvedValue();

      try {
        await service.connectLocal();
      } catch (error) {
      }

      expect(sleepSpy).toHaveBeenCalledWith(1000);
      expect(sleepSpy).toHaveBeenCalledWith(2000);
      expect(sleepSpy).toHaveBeenCalledTimes(2);
    });
  });

  describe('connectionService singleton', () => {
    beforeEach(() => {
      connectionService.isInitialized = false;
      connectionService.localPool = null;
      connectionService.localClient = null;
    });

    test('should be an instance of ConnectionService', () => {
      expect(connectionService).toBeInstanceOf(ConnectionService);
    });

    test('should maintain state across imports', async () => {
      const result = await connectionService.initialize();

      expect(result.success).toBe(true);
      expect(connectionService.isInitialized).toBe(true);
    });
  });
});
