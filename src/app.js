const express = require('express');
const helmet = require('helmet');
const cors = require('cors');
const morgan = require('morgan');

const walmartRoutes = require('./routes/walmart');

const app = express();

// Security and logging middleware
app.use(helmet());
app.use(cors());
app.use(morgan('combined'));
app.use(express.json());

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'oms-walmart-service' });
});

// Routes
app.use('/walmart', walmartRoutes);

// 404 handler
app.use((req, res) => {
  res.status(404).json({ error: { code: 'NOT_FOUND', message: 'Route not found' } });
});

// Error handler
app.use((err, req, res, next) => {
  console.error(err);
  res.status(500).json({ error: { code: 'INTERNAL_ERROR', message: 'Internal server error' } });
});

module.exports = app;
