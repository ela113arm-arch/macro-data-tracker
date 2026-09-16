const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('templates/index.html', 'utf8');
const ctx = {};
vm.runInNewContext(source.slice(source.indexOf('const finiteNumberOrNull ='), source.indexOf('const latestDateFor =')) + source.slice(source.indexOf('function monthlyInflationYoY('), source.indexOf('async function loadPolicyInflationLabor()')), ctx);
const yoy = rows => Array.from(ctx.monthlyInflationYoY(rows, 'index'));
test('blank and zero levels stay missing, never become -100% inflation', () => {
  for (const missing of ['', ' ', null, undefined, 0]) {
    assert.deepEqual(yoy([{ date:'2024-10-01',index:100 },{date:'2025-10-01',index:missing}]), [null,null]);
  }
});
test('annual change matches the calendar month even with missing rows', () => {
  const actual=yoy([{date:'2024-01-01',index:100},{date:'2024-03-01',index:200},{date:'2025-01-01',index:105},{date:'2025-02-01',index:110},{date:'2025-03-01',index:190}]);
  assert.equal(actual[3],null);
  assert.ok(Math.abs(actual[2]-5)<1e-10);
  assert.ok(Math.abs(actual[4]+5)<1e-10);
});
test('missing prior-year observation stays missing', () => {
  assert.deepEqual(yoy([{date:'2024-01-01',index:''},{date:'2025-01-01',index:105}]),[null,null]);
});
