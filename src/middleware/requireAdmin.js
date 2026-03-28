function requireAdmin(req, res, next) {
  if (req.userRole !== 'admin') {
    return res.status(403).json({
      error: { code: 'FORBIDDEN', message: 'Admin access required' },
    });
  }
  next();
}

module.exports = requireAdmin;
