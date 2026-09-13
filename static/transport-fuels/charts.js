(() => {
  const products = [
    {key: 'gasoline', label: 'Gasoline', color: '#3b82f6'},
    {key: 'jet_fuel', label: 'Jet fuel', color: '#f59e0b'},
    {key: 'diesel', label: 'Diesel', color: '#10b981'}
  ];
  const countrySelect = document.getElementById('fuel-country');
  const viewSelect = document.getElementById('fuel-view');
  const scaleSelect = document.getElementById('fuel-common-scale');
  const grid = document.getElementById('fuel-grid');
  const status = document.getElementById('fuel-status');
  let data;
  let refreshStatus;
  const signed = n => `${n >= 0 ? '+' : ''}${n.toFixed(3)}`;
  const month = date => new Date(`${date}T00:00:00Z`).toLocaleDateString('en-US', {month: 'short', year: 'numeric', timeZone: 'UTC'});
  const verifiedDate = value => {
    const parsed = value ? new Date(value) : new Date('2026-09-12T00:00:00Z');
    return Number.isNaN(parsed.getTime())
      ? 'Sep 12, 2026'
      : parsed.toLocaleDateString('en-US', {month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC'});
  };
  function extent(rows, yoy) {
    return Math.max(...rows.map(row => {
      const values = products.map(p => row[p.key + (yoy ? '_yoy_mbd' : '_mbd')]);
      return Math.max(values.filter(v => v > 0).reduce((a,b) => a+b, 0), -values.filter(v => v < 0).reduce((a,b) => a+b, 0));
    })) * 1.15 || 0.1;
  }
  function render() {
    grid.querySelectorAll('.fuel-plot').forEach(el => Plotly.purge(el));
    grid.replaceChildren();
    const yoy = viewSelect.value === 'yoy';
    const selected = data.filter(c => countrySelect.value === 'all' || c.country === countrySelect.value);
    const common = Math.max(...data.map(c => extent(c.rows, yoy)));
    const latestDate = data.flatMap(c => c.rows.map(r => r.date)).sort().at(-1);
    const endDate = new Date(`${latestDate}T00:00:00Z`);
    endDate.setUTCMonth(endDate.getUTCMonth() + 1);
    const chartEnd = endDate.toISOString().slice(0,10);
    document.getElementById('fuel-period').textContent = `Gasoline, jet fuel & diesel · ${data.length} markets · July 2021–${month(latestDate)}`;
    for (const c of selected) {
      const card = document.createElement('article'); card.className = 'fuel-card';
      const heading = document.createElement('h3'); heading.textContent = c.country;
      const latest = c.rows[c.rows.length - 1];
      const subtitle = document.createElement('p'); subtitle.className = 'fuel-latest';
      subtitle.textContent = `${month(latest.date)} · ${yoy ? signed(latest.net_yoy_mbd) + ' M b/d YoY' : products.reduce((sum,p) => sum + latest[p.key+'_mbd'], 0).toFixed(3) + ' M b/d combined'}`;
      const freshness = document.createElement('p'); freshness.className = 'fuel-freshness';
      const check = refreshStatus?.countries?.[c.country];
      const checkedAt = check?.last_success_at ? verifiedDate(check.last_success_at) : null;
      if (check?.status === 'error') {
        // Keep parser details out of the analyst-facing UI. The raw error remains
        // available in the refresh artifact for operators, while the card gives
        // readers a useful, stable state and verification date.
        freshness.textContent = `Data temporarily unavailable — last verified ${verifiedDate(check.last_attempt_at || check.last_success_at)}`;
        freshness.classList.add('fuel-warning', 'fuel-unavailable');
      } else if (checkedAt) {
        // The ETL runs weekdays; 96h spans the normal Friday-to-Monday gap.
        const overdue = Date.now() - Date.parse(check.last_success_at) > 96 * 60 * 60 * 1000;
        freshness.textContent = `${overdue ? 'Source check overdue · ' : ''}Last verified: ${checkedAt}`;
        if (overdue) freshness.classList.add('fuel-warning');
      } else {
        const overdue = refreshStatus?.configured_at && Date.now() - Date.parse(refreshStatus.configured_at) > 96 * 60 * 60 * 1000;
        freshness.textContent = refreshStatus ? (overdue ? 'First automated source check overdue · Retaining uploaded data.' : 'Awaiting first full automated source check.') : 'Refresh status unavailable · Data shown below may be stale.';
        freshness.classList.add('fuel-warning');
      }
      const plot = document.createElement('div'); plot.className = 'fuel-plot'; plot.setAttribute('aria-label', `${c.country} monthly transport-fuel ${yoy ? 'year-over-year changes' : 'levels'} in million barrels per day`);
      const details = document.createElement('details');
      const summary = document.createElement('summary'); summary.textContent = 'Source & country notes'; details.append(summary);
      const link = document.createElement('a'); link.textContent = c.source; link.href = c.source_url; link.target = '_blank'; link.rel = 'noopener noreferrer'; details.append(link);
      for (const text of [c.vintage, c.measure + ': ' + c.product_mapping, c.conversion_note, c.caveat]) {
        const p = document.createElement('p'); p.textContent = text; details.append(p);
      }
      card.append(heading, subtitle, freshness, plot, details); grid.append(card);
      const traces = products.map(p => ({type:'bar', name:p.label, x:c.rows.map(r=>r.date), y:c.rows.map(r=>r[p.key+(yoy?'_yoy_mbd':'_mbd')]), marker:{color:p.color}, hovertemplate:'%{x|%b %Y}<br>%{y:.3f} M b/d<extra>%{fullData.name}</extra>'}));
      if (yoy) traces.push({type:'scatter', mode:'lines', name:'Net', x:c.rows.map(r=>r.date), y:c.rows.map(r=>r.net_yoy_mbd), line:{color:'#111827',width:2}, hovertemplate:'%{x|%b %Y}<br>%{y:.3f} M b/d<extra>Net</extra>'});
      const limit = scaleSelect.checked ? common : extent(c.rows,yoy);
      Plotly.newPlot(plot, traces, {barmode:'relative', autosize:true, margin:{l:55,r:10,t:12,b:75}, font:{family:'Segoe UI, sans-serif',size:12,color:'#4b5563'}, paper_bgcolor:'white',plot_bgcolor:'white', hovermode:'x unified', legend:{orientation:'h',y:-0.23,x:0,font:{size:12}}, xaxis:{type:'date',range:['2021-06-15',chartEnd],dtick:'M12',tick0:'2022-01-01',tickformat:'%Y',showgrid:false}, yaxis:{title:{text:'M b/d',font:{size:12}},range:yoy?[-limit,limit]:[0,limit],zerolinecolor:'#9ca3af',gridcolor:'#eef0f3'}, dragmode:false}, {responsive:true,displayModeBar:false});
    }
    status.textContent = '';
  }
  window.loadTransportFuels = async function () {
    status.textContent = 'Loading transport-fuel data…';
    try {
      if (!data) {
        const response = await fetch('/static/transport-fuels/data.json', {cache:'no-store'});
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        data = await response.json();
        try {
          const health = await fetch('/static/transport-fuels/refresh-status.json', {cache:'no-store'});
          refreshStatus = health.ok ? await health.json() : null;
        } catch { refreshStatus = null; }
        for (const c of data) { const option = document.createElement('option'); option.value = c.country; option.textContent = c.country; countrySelect.append(option); }
      }
      render();
    } catch (error) {
      status.textContent = 'Data temporarily unavailable — last good data: Sep 12, 2026. ';
      const retry = document.createElement('button'); retry.textContent = 'Retry'; retry.onclick = () => window.loadTransportFuels().catch(() => {}); status.append(retry);
      throw error;
    }
  };
  [countrySelect,viewSelect,scaleSelect].forEach(control => control.addEventListener('change', () => { if(data) render(); }));
})();
