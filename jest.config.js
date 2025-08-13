export default {
  testEnvironment: 'node',
  transform: {},
  collectCoverageFrom: ['src/**/*.js', '!src/**/*.test.js'],

  testMatch: ['**/tests/**/*.test.js'],
  verbose: true,
  setupFilesAfterEnv: ['<rootDir>/tests/setup.js']
};
