const { RedisJobWorker } = require('./redis-client');

// Start the worker
async function main() {
  console.log('Starting worker with Redis job system');
  const redisWorker = new RedisJobWorker();
  
  try {
    await redisWorker.init();
    await redisWorker.start();
  } catch (error) {
    console.error('Worker failed to start:', error);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}