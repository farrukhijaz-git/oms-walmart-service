function requireUser(req, res, next) {
  const userId = req.headers['x-user-id'];
  const userRole = req.headers['x-user-role'];

  if (!userId || !userRole) {
    return res.status(401).json({
      error: { code: 'UNAUTHORIZED', message: 'Missing user context' },
    });
  }

  req.userId = userId;
  req.userRole = userRole;
  next();
}

module.exports = requireUser;
