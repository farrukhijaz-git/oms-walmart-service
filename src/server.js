require('dotenv').config();

const app = require('./app');
const { startScheduler } = require('./services/scheduler');

const PORT = process.env.PORT || 3004;

app.listen(PORT, () => {
  console.log(`oms-walmart-service listening on port ${PORT}`);
  startScheduler().catch(err => {
    console.error('Failed to start scheduler:', err.message);
  });
});
