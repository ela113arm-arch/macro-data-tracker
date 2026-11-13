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
  function seasonPosition(value, weekly) {
    const d = new Date(value + 'T12:00:00Z');
    if (weekly) {
      // ISO weeks keep complete Monday–Sunday reports together at New Year.
      const thursday = new Date(d);
      thursday.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7));
      const year = thursday.getUTCFullYear();
      const start = Date.UTC(year, 0, 1, 12);
      return {year, position: Math.ceil((((thursday.getTime() - start) / 86400000) + 1) / 7)};
    }
    if (d.getUTCMonth() === 1 && d.getUTCDate() === 29) return null;
    // A non-leap reference calendar keeps March–December aligned in every year.
    return {year: d.getUTCFullYear(), position: Math.floor((Date.UTC(2001, d.getUTCMonth(), d.getUTCDate()) - Date.UTC(2001, 0, 1)) / 86400000) + 1};
  }
  function seasonalData(rows, weekly, currentYear, smooth = true) {
    const combined = new Map(rows.map(r => [r.date, r]));
    // References carry their actual observation dates; never relabel them as current data.
    rows.forEach(r => {
      if (r.reference_date && Number.isFinite(r.reference_value) && !combined.has(r.reference_date)) {
        combined.set(r.reference_date, {date: r.reference_date, value: r.reference_value});
      }
    });
    const observations = Array.from(combined.values()).sort((a, b) => a.date.localeCompare(b.date));
    const values = !weekly && smooth ? sevenDayMean(observations) : observations.map(r => Number(r.value));
    const years = new Map();
    observations.forEach((r, i) => {
      const pos = seasonPosition(r.date, weekly);
      if (!pos || !Number.isFinite(values[i])) return;
      if (!years.has(pos.year)) years.set(pos.year, new Map());
      years.get(pos.year).set(pos.position, {value: values[i], date: r.date});
    });
    const count = weekly ? 53 : 365;
    const x = Array.from({length: count}, (_, i) => i + 1);
    const historyYears = Array.from(years.keys()).filter(y => y >= currentYear - 5 && y < currentYear).sort();
    const current = x.map(p => years.get(currentYear)?.get(p)?.value ?? null);
    const prior = x.map(p => years.get(currentYear - 1)?.get(p)?.value ?? null);
    const currentDates = x.map(p => years.get(currentYear)?.get(p)?.date ?? '');
    const priorDates = x.map(p => years.get(currentYear - 1)?.get(p)?.date ?? '');
    const low = [], high = [], average = [], samples = [];
    x.forEach(p => {
      const v = historyYears.map(y => years.get(y).get(p)?.value).filter(Number.isFinite);
      samples.push(v.length);
      // One observed year is a comparison line, not a historical range.
      low.push(v.length >= 2 ? Math.min(...v) : null);
      high.push(v.length >= 2 ? Math.max(...v) : null);
      average.push(v.length >= 2 ? v.reduce((a, b) => a + b, 0) / v.length : null);
    });
    return {x, current, prior, currentDates, priorDates, low, high, average, samples, historyYears};
  }
  function seasonalTraces(data, year) {
    const traces = [];
    if (data.low.some(Number.isFinite)) {
      traces.push({x: data.x, y: data.low, type: 'scatter', mode: 'lines', line: {width: 0}, showlegend: false, hoverinfo: 'skip', connectgaps: false});
      traces.push({x: data.x, y: data.high, type: 'scatter', mode: 'lines', line: {width: 0}, fill: 'tonexty', fillcolor: 'rgba(148,163,184,0.22)', name: 'Historical range', hoverinfo: 'skip', connectgaps: false});
      traces.push({x: data.x, y: data.average, type: 'scatter', mode: 'lines', name: 'Historical average',
        customdata: data.samples, connectgaps: false, line: {color: '#7e8794', width: 1.5, dash: 'dot'},
        hovertemplate: '%{y:,.0f}<br>%{customdata} historical years<extra>Historical average</extra>'});
    }
    if (data.prior.some(Number.isFinite)) traces.push({x: data.x, y: data.prior, customdata: data.priorDates,
      name: String(year - 1), type: 'scatter', mode: 'lines', connectgaps: false, line: {color: '#2563eb', width: 2},
      hovertemplate: '%{customdata}<br>%{y:,.0f}<extra>' + (year - 1) + '</extra>'});
    traces.push({x: data.x, y: data.current, customdata: data.currentDates, name: String(year), type: 'scatter', mode: 'lines',
      connectgaps: false, line: {color: '#e87b24', width: 2.7}, hovertemplate: '%{customdata}<br>%{y:,.0f}<extra>' + year + '</extra>'});
    return traces;
  }
  function warning(card, message) {
    card.append(element('p', 'demand-warning', message));
  }

  function draw() {
    const grid = document.getElementById('demand-grid');
    grid.querySelectorAll('.js-plotly-plot').forEach(el => Plotly.purge(el));
    grid.replaceChildren();
    const smooth = document.getElementById('demand-view').value === 'average';
    const currentYear = new Date().getUTCFullYear();
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
      const data = seasonalData(rows, weekly, currentYear, smooth);
      const legend = element('div', 'demand-legend');
      const legendItems = [[String(currentYear), 'current']];
      if (data.prior.some(Number.isFinite)) legendItems.push([String(currentYear - 1), 'prior']);
      if (data.low.some(Number.isFinite)) legendItems.push(['Historical average', 'average'], ['Historical range', 'range']);
      legendItems.forEach(([label, style]) => {
        const item = element('span', 'demand-legend-item');
        const swatch = element('i', 'demand-swatch demand-swatch-' + style);
        swatch.setAttribute('aria-hidden', 'true');
        item.append(swatch, document.createTextNode(label)); legend.append(item);
      });
      card.append(legend);
      const plot = element('div', 'demand-chart'); plot.id = 'demand-chart-' + id;
      plot.setAttribute('role', 'img'); plot.setAttribute('aria-label', series.title + ', ' + series.unit);
      card.append(plot);
      const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
      const monthDays = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335];
      const tickValues = weekly ? monthDays.map(d => 1 + (d - 1) / 7) : monthDays;
      const windowLabel = data.historyYears.length ? data.historyYears.join(', ') : 'not yet available';
      const statLabel = data.historyYears.length === 5 ? '5-year range and average: ' : 'Available historical range and average: ';
      const method = weekly ? 'Weekly totals · ISO week alignment. ' : (smooth ? '7-day averages' : 'Daily values') + ' · Calendar dates aligned; February 29 omitted. ';
      card.append(element('p', 'demand-caption', method + (data.historyYears.length >= 2 ? statLabel + windowLabel + '.' : 'Prior-year comparison: ' + windowLabel + '; a multi-year range is unavailable.')));
      if (!data.prior.some(Number.isFinite)) warning(card, 'Prior-year observations are not yet available. The seasonal chart shows the current year only.');
      const last = data.current.findLastIndex(Number.isFinite);
      const annotations = last < 0 ? [] : [{x: data.x[last], y: data.current[last], text: number.format(data.current[last]),
        showarrow: false, xanchor: data.x[last] > (weekly ? 45 : 310) ? 'right' : 'left', yshift: 13,
        bgcolor: 'rgba(255,255,255,0.9)', font: {color: '#c45f13', size: 10}}];
      Plotly.newPlot(plot, seasonalTraces(data, currentYear), {autosize: true, height: 330, showlegend: false, margin: {t: 24, r: 18, b: 42, l: 56},
        paper_bgcolor: '#fff', plot_bgcolor: '#fff', font: {family: 'Inter, Arial, sans-serif', size: 11, color: '#475569'},
        xaxis: {type: 'linear', range: weekly ? [1, 53] : [1, 365], tickmode: 'array', tickvals: tickValues, ticktext: monthNames,
          showgrid: false, tickfont: {size: 10}}, yaxis: {gridcolor: '#edf0f4', fixedrange: false, separatethousands: true},
        annotations, hovermode: 'x unified'},
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
      document.getElementById('demand-view').onchange = draw;
    } catch (error) {
      status.textContent = 'Transport data could not be loaded. Reload the page to retry.';
      document.getElementById('demand-grid').replaceChildren();
    } finally { clearTimeout(timer); }
  };
  // Export only the pure calculation used by the small Node regression test.
  if (typeof module !== 'undefined' && module.exports) module.exports = {sevenDayMean, seasonPosition, seasonalData, seasonalTraces};
})();
