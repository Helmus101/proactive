const { clearZeroBaseMemory } = require('./graph-store');
function graphDerivation() {
  // lazy require to avoid circular dependency at load time
  // eslint-disable-next-line global-require
  return require('./graph-derivation');
}

async function resetZeroBaseMemory({
  includeEvents = true,
  rederive = false
} = {}) {
  await clearZeroBaseMemory({ includeEvents });
  if (!rederive) {
    return {
      reset: true,
      includeEvents,
      rederived: false
    };
  }

  const result = await graphDerivation().deriveGraphFromEvents({
    versionSeed: 'current'
  });
  return {
    reset: true,
    includeEvents,
    rederived: true,
    ...result
  };
}

module.exports = {
  resetZeroBaseMemory
};
