const mockLogger = {
  info: jest.fn(),
  error: jest.fn(),
  warn: jest.fn(),
  debug: jest.fn(),
  success: jest.fn(),
  startSpinner: jest.fn(() => ({ stop: jest.fn() })),
  updateSpinner: jest.fn(),
  stopSpinner: jest.fn(),
  createProgressBar: jest.fn(() => ({ 
    start: jest.fn(), 
    update: jest.fn(), 
    stop: jest.fn() 
  })),
  updateProgress: jest.fn(),
  stopProgress: jest.fn(),
  section: jest.fn(),
  subsection: jest.fn(),
  table: jest.fn(),
  summary: jest.fn(),
  cleanup: jest.fn()
};

export const Logger = jest.fn(() => mockLogger);
export const logger = mockLogger;
