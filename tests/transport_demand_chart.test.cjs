const {test} = require('node:test');
const assert = require('node:assert/strict');
global.window = {};
const {sevenDayMean, seasonPosition, seasonalData} = require('../static/transport-demand/charts.js');
test('averages require seven consecutive calendar days, including outside the visible range', () => {
  const rows = Array.from({length: 8}, (_, i) => ({date: `2026-11-${String(i + 1).padStart(2, '0')}`, value: i + 1}));
  assert.deepEqual(sevenDayMean(rows), [null, null, null, null, null, null, 4, 5]);
  rows.splice(3, 1);
  assert.deepEqual(sevenDayMean(rows), [null, null, null, null, null, null, null]);
});
test('seasonal statistics use the five completed years and do not extend the current year', () => {
  const rows = [{date: '2020-07-01', value: 9999}];
  for (let year = 2021; year <= 2026; year++) rows.push({date: `${year}-07-01`, value: (year - 2020) * 10});
  const d = seasonalData(rows, false, 2026, false);
  const i = seasonPosition('2026-07-01', false).position - 1;
  assert.deepEqual(d.historyYears, [2021, 2022, 2023, 2024, 2025]);
  assert.equal(d.low[i], 10); assert.equal(d.high[i], 50); assert.equal(d.average[i], 30);
  assert.equal(d.prior[i], 50); assert.equal(d.current[i], 60);
  assert.ok(d.current.slice(i + 1).every(x => x === null));
});
test('a single historical observation is not represented as a multi-year range', () => {
  const d = seasonalData([{date: '2025-06-02', value: 10}, {date: '2026-06-02', value: 20}], false, 2026, false);
  const i = seasonPosition('2026-06-02', false).position - 1;
  assert.equal(d.prior[i], 10); assert.equal(d.samples[i], 1);
  assert.equal(d.low[i], null); assert.equal(d.high[i], null); assert.equal(d.average[i], null);
});
test('calendar alignment preserves March dates across leap years and omits February 29', () => {
  assert.equal(seasonPosition('2024-02-29', false), null);
  assert.equal(seasonPosition('2024-03-01', false).position, seasonPosition('2025-03-01', false).position);
});
test('complete weekly reports align by ISO week including New Year boundaries', () => {
  assert.deepEqual(seasonPosition('2023-01-01', true), {year: 2022, position: 52});
  assert.deepEqual(seasonPosition('2023-01-08', true), {year: 2023, position: 1});
  assert.deepEqual(seasonPosition('2026-01-04', true), {year: 2026, position: 1});
});
test('source reference observations retain their actual prior-year dates', () => {
  const d = seasonalData([{date: '2026-01-01', value: 20, reference_date: '2025-01-02', reference_value: 19}], false, 2026, false);
  assert.equal(d.current[0], 20); assert.equal(d.prior[0], null); assert.equal(d.prior[1], 19);
});
