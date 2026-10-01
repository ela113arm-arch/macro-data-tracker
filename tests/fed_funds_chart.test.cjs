const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('templates/index.html', 'utf8');
const ctx = {};
vm.runInNewContext(source.slice(source.indexOf('const finiteNumberOrNull ='), source.indexOf('const signedValue =')) + ';globalThis.trim = trimTrailingSeries; globalThis.latest = latestDateFor;', ctx);
test('fed funds ends at its own observation, even when Treasury yields are newer', () => {
    const rows = [
        {date: '2026-09-16', fed_funds: 3.63},
        {date: '2026-09-17', fed_funds: 3.88},
        {date: '2026-09-18', fed_funds: null},
        {date: '2026-09-21', fed_funds: ''},
    ];
    const points = ctx.trim(rows, 'fed_funds');
    assert.equal(points.length, 2);
    assert.equal(points.at(-1).date, '2026-09-17');
    assert.equal(points.at(-1).value, 3.88);
    assert.equal(ctx.latest(rows, 'fed_funds'), '2026-09-17');
});
test('missing fed funds remains unavailable rather than becoming zero', () => {
    assert.equal(ctx.trim([{date: '2026-09-21', fed_funds: null}], 'fed_funds').length, 0);
    assert.equal(ctx.latest([{date: '2026-09-21', fed_funds: null}], 'fed_funds'), null);
});
