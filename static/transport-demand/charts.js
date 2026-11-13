/* Public transport activity indicators; no fuel-volume conversion. */
(function () {
  'use strict';
  const ids = ['tsa_passengers', 'europe_flights', 'china_trucks', 'china_flights'];
  const titles = ['U.S. airport passengers', 'European flight activity', 'China highway truck traffic', 'China flight activity'];
  const number = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });
  let payload;

  function dateLabel(value) {
    if (!value) return 'Not provided';
    const d = new Date(value.length === 10 ? value + 'T12:00:00Z' : value);
    return Number.isNaN(d.getTime()) ? 'Not provided' : d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' });
  }
  function element(tag, className, text) {
    const el = document.createElement(tag);
    if (className) el.className = className;
    if (text !== undefined) el.textContent = text;
    return el;
  }
  function link(label, url) {
    const a = element('a', '', label);
    a.href = url; a.target = '_blank'; a.rel = 'noopener';
    return a;
  }
  function sevenDayMean(rows) {
    const lookup = new Map(rows.map(r => [r.date, Number(r.value)]));
    return rows.map(r => {
      let sum = 0;
      for (let i = 0; i < 7; i++) {
        const day = new Date(r.date + 'T12:00:00Z');
        day.setUTCDate(day.getUTCDate() - i);
        const value = lookup.get(day.toISOString().slice(0, 10));
        if (!Number.isFinite(value)) return null;
        sum += value;
      }
      return sum / 7;
    });
  }
  function warning(card, message) {
    card.append(element('p', 'demand-warning', message));
  }

  function draw() {
    const grid = document.getElementById('demand-grid');
    grid.querySelectorAll('.js-plotly-plot').forEach(el => Plotly.purge(el));
    grid.replaceChildren();
    const history = document.getElementById('demand-range').value;
    ids.forEach((id, index) => {
      const series = payload.series[id];
      const feed = payload.feeds[series ? series.feed : ['tsa', 'eurocontrol', 'china', 'china'][index]] || {};
      const card = element('article', 'demand-card');
      const top = element('div', 'demand-card-top');
      const weekly = series ? series.frequency === 'weekly' : index > 1;
      top.append(element('span', 'demand-badge' + (weekly ? ' demand-badge-weekly' : ''), weekly ? 'Weekly · China exception' : 'Daily observations'));
      if (series) {
        const a = link('Download CSV', '/static/transport-demand/' + id + '.csv');
        a.removeAttribute('target'); a.setAttribute('download', ''); top.append(a);
      }
      card.append(top, element('h3', '', series ? series.title : titles[index]));
      grid.append(card);
      if (!series || !series.rows.length) {
        warning(card, 'Data unavailable. The next daily source check will retry.');
        return;
      }
      const rows = series.rows;
      const latest = rows[rows.length - 1];
      card.append(element('div', 'demand-value', number.format(latest.value)));
      card.append(element('p', 'demand-caption', series.unit + ' · ' + series.fuel + ' activity indicator'));
      const observed = weekly ? dateLabel(latest.period_start) + ' – ' + dateLabel(latest.date) : dateLabel(latest.date);
      card.append(element('p', 'demand-dates', 'Latest observation: ' + observed));
      const released = weekly ? latest.published_date : feed.source_published_date;
      card.append(element('p', 'demand-dates', (released ? (feed.source_name === 'TSA via Passenger Index' ? 'Source page updated: ' : 'Source release: ') + dateLabel(released) + ' · ' : '') + 'Last successful check: ' + dateLabel(feed.last_success_at)));
      const ageDays = Math.floor((Date.now() - Date.parse(latest.date + 'T00:00:00Z')) / 86400000);
      const checkAge = feed.last_success_at ? (Date.now() - Date.parse(feed.last_success_at)) / 86400000 : Infinity;
      if (feed.status !== 'ok') warning(card, 'Latest source check failed. Showing the last validated observations; see the linked source for updates.');
      if (ageDays > (weekly ? 14 : 4) || checkAge > 2) warning(card, 'Refresh overdue. Latest observation is ' + ageDays + ' days old; last successful check was ' + dateLabel(feed.last_success_at) + '.');
      if (feed.notice && !weekly) warning(card, feed.notice);
      const plot = element('div', 'demand-chart'); plot.id = 'demand-chart-' + id;
      plot.setAttribute('role', 'img'); plot.setAttribute('aria-label', series.title + ', ' + series.unit);
      card.append(plot);
      let shown = rows;
      if (history !== 'all') {
        const cutoff = new Date(latest.date + 'T12:00:00Z'); cutoff.setUTCDate(cutoff.getUTCDate() - Number(history));
        shown = rows.filter(r => r.date >= cutoff.toISOString().slice(0, 10));
      }
      const traces = [{x: shown.map(r => r.date), y: shown.map(r => r.value), name: weekly ? 'Weekly total' : 'Daily',
        type: 'scatter', mode: weekly ? 'lines+markers' : 'lines', connectgaps: false,
        line: {color: weekly ? '#1a465b' : '#a6b5c5', width: weekly ? 2.4 : 1.1}, marker: {size: 4},
        hovertemplate: '%{x}<br>%{y:,.0f}<extra>' + (weekly ? 'Weekly total' : 'Daily') + '</extra>'}];
      if (!weekly) {
        const sevenDayValues = sevenDayMean(rows);
        const means = new Map(rows.map((r, i) => [r.date, sevenDayValues[i]]));
        traces.push({x: shown.map(r => r.date), y: shown.map(r => means.get(r.date)), name: '7-day average',
          type: 'scatter', mode: 'lines', connectgaps: false, line: {color: '#1a465b', width: 2.6}, hovertemplate: '%{x}<br>%{y:,.0f}<extra>7-day average</extra>'});
      }
      const reference = shown.filter(r => Number.isFinite(r.reference_value));
      if (reference.length) traces.push({x: reference.map(r => r.date), y: reference.map(r => r.reference_value),
        customdata: reference.map(r => r.reference_date), name: 'Prior year · matched weekday', type: 'scatter', mode: 'lines',
        line: {color: '#c28a35', width: 1.5, dash: 'dot'}, hovertemplate: 'Reference: %{customdata}<br>%{y:,.0f}<extra>Prior year</extra>'});
      Plotly.newPlot(plot, traces, {autosize: true, height: 300, margin: {t: 8, r: 12, b: 64, l: 56},
        paper_bgcolor: '#fff', plot_bgcolor: '#fff', font: {family: 'Inter, Arial, sans-serif', size: 11, color: '#475569'},
        xaxis: {type: 'date', showgrid: false, nticks: 5}, yaxis: {rangemode: 'tozero', gridcolor: '#edf0f4', fixedrange: false},
        legend: {orientation: 'h', x: 0, y: -0.22, font: {size: 10}}, hovermode: 'x unified'},
        {responsive: true, displayModeBar: false}).catch(() => { plot.textContent = 'Chart could not be rendered. The CSV download remains available.'; });
      card.append(element('p', 'demand-caption', series.description));
      const source = element('p', 'demand-attribution', feed.attribution + ' ');
      source.append(link('Source', latest.source_url || feed.source_url)); card.append(source);
    });
  }

  window.loadTransportDemand = async function () {
    const status = document.getElementById('demand-run-status');
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 15000);
    try {
      const response = await fetch('/static/transport-demand/data.json', {signal: controller.signal, cache: 'no-store'});
      if (!response.ok) throw new Error('Data request failed');
      payload = await response.json();
      if (payload.version !== 1 || !payload.series || !payload.feeds) throw new Error('Unrecognised data format');
      status.textContent = 'Latest pipeline check: ' + dateLabel(payload.last_attempt_at) + ' · Observation dates are shown on each chart.';
      draw();
      document.getElementById('demand-range').onchange = draw;
    } catch (error) {
      status.textContent = 'Transport data could not be loaded. Reload the page to retry.';
      document.getElementById('demand-grid').replaceChildren();
    } finally { clearTimeout(timer); }
  };
  // Export only the pure calculation used by the small Node regression test.
  if (typeof module !== 'undefined' && module.exports) module.exports = {sevenDayMean};
})();
