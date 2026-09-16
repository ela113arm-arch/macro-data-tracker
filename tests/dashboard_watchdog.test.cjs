const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('templates/index.html', 'utf8');
const watchdog = source.slice(source.indexOf('function startChartWatchdog()'), source.indexOf('function legendTraces'));

function tick({ rendered = false, visible = true, error = false } = {}) {
  let removed = false, failed = false;
  const host = {
    dataset: { loadingStartedAt: '1', ...(error ? { dashboardError: 'true' } : {}) },
    querySelector: () => rendered ? {} : null,
  };
  const loading = { closest: () => host, remove: () => { removed = true; } };
  vm.runInNewContext(watchdog, {
    window: { setInterval: callback => callback() },
    document: { querySelectorAll: () => [loading] },
    chartHostIsVisible: () => visible,
    showChartError: () => { failed = true; },
    DASHBOARD_FETCH_TIMEOUT_MS: 10000,
    Date: { now: () => 20000 },
  });
  return { removed, failed, host };
}
test('rendered charts survive expired loading placeholders', () => {
  const result = tick({ rendered: true });
  assert.equal(result.failed, false);
  assert.equal(result.removed, true);
  assert.equal(result.host.dataset.loadingStartedAt, undefined);
});
test('genuinely pending visible charts still time out', () => {
  assert.equal(tick().failed, true);
});
test('hidden charts do not time out', () => {
  const result = tick({ visible: false });
  assert.equal(result.failed, false);
  assert.equal(result.host.dataset.loadingStartedAt, undefined);
});
test('successful rendering removes loading and previous error elements', async () => {
  let removed = 0;
  const host = { dataset: { dashboardError: 'true', loadingStartedAt: '1' }, querySelectorAll: selector => {
    assert.equal(selector, '.loading, .chart-error');
    return [{ remove: () => removed++ }, { remove: () => removed++ }];
  }};
  const context = {
    Plotly: { newPlot: async () => host }, document: { getElementById: () => host },
    cleanLayout: x => x, legendTraces: () => [], window: { innerWidth: 1200 },
    ensureMobileLegend() {}, plotlyConfig: {}, resizeVisibleCharts() {},
    showChartError() { assert.fail('successful render reported as failed'); },
  };
  vm.runInNewContext(source.slice(source.indexOf('const originalNewPlot'), source.indexOf('const fmt =')), context);
  await context.Plotly.newPlot('chart', [], {});
  assert.equal(removed, 2);
  assert.deepEqual(host.dataset, {});
});
