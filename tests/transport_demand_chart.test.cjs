const {test} = require('node:test');
const assert = require('node:assert/strict');
global.window = {};
const {sevenDayMean} = require('../static/transport-demand/charts.js');
test('averages require seven consecutive calendar days, including outside the visible range', () => {
  const rows = Array.from({length: 8}, (_, i) => ({date: `2026-11-${String(i + 1).padStart(2, '0')}`, value: i + 1}));
  assert.deepEqual(sevenDayMean(rows), [null, null, null, null, null, null, 4, 5]);
  rows.splice(3, 1);
  assert.deepEqual(sevenDayMean(rows), [null, null, null, null, null, null, null]);
});
