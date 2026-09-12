"""Official Brazil, Mexico and Australia transport-fuel adapters.

Brazil has a stable open CSV. Mexico and Australia remain explicit failures until
an official current export can be parsed and validated; the coordinator retains
those countries' last good data rather than silently substituting another measure.
"""
from datetime import date, datetime, timezone
import io
import re
import unicodedata
import pandas as pd
from . import START, PRODUCTS, iso_month, parse_number, validate_rows

ANP_LANDING = 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos'
ANP_URLS = [
 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vendas-derivados-petroleo-e-etanol/vendas-combustiveis-m3-1990-2026.csv',
 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vendas-derivados-petroleo-e-etanol/vendas-combustiveis-m3-1990-2025.csv',
]

def _get(session,url):
 r=session.get(url,timeout=(10,90));r.raise_for_status()
 if not r.content: raise ValueError('Empty official response')
 return r

def _norm(v):
 return ''.join(c for c in unicodedata.normalize('NFKD',str(v or '')) if not unicodedata.combining(c)).upper().strip()

def fetch_brazil(session,today=None):
 response=None;url=None
 for candidate in ANP_URLS:
  try: response=_get(session,candidate);url=candidate;break
  except Exception: continue
 if response is None: raise ValueError('ANP fuel-sales CSV unavailable at current or prior official endpoint')
 raw=pd.read_csv(io.BytesIO(response.content),sep=';',decimal=',',encoding='utf-8-sig')
 expected={'ANO','MÊS','GRANDE REGIÃO','UNIDADE DA FEDERAÇÃO','PRODUTO','VENDAS'}
 if not expected.issubset(raw.columns): raise ValueError(f'ANP CSV missing columns: {sorted(expected-set(raw.columns))}')
 products={_norm('GASOLINA C'):'gasoline',_norm('QUEROSENE DE AVIAÇÃO'):'jet_fuel',_norm('ÓLEO DIESEL'):'diesel'}
 raw['product_key']=raw['PRODUTO'].map(_norm).map(products)
 raw=raw[raw['product_key'].notna()].copy()
 states=set(raw['UNIDADE DA FEDERAÇÃO'].dropna().map(_norm))
 if len(states)<20: raise ValueError(f'ANP expected 27 federative units; found {len(states)}')
 months={'JAN':1,'FEV':2,'MAR':3,'ABR':4,'MAI':5,'JUN':6,'JUL':7,'AGO':8,'SET':9,'OUT':10,'NOV':11,'DEZ':12}
 raw['month_num']=raw['MÊS'].map(lambda x:months.get(_norm(x)))
 if raw['month_num'].isna().any(): raise ValueError('ANP month labels changed')
 raw['date']=raw.apply(lambda r:f"{int(r['ANO']):04d}-{int(r['month_num']):02d}-01",axis=1)
 raw['value']=raw['VENDAS'].map(parse_number)
 if raw['value'].isna().any() or (raw['value']<0).any(): raise ValueError('ANP contains invalid sales values')
 # The file is state-level. Aggregate only the 27 federative units, never
 # regional/national subtotal rows that may appear in future releases.
 valid_states={_norm(x) for x in ('ACRE','ALAGOAS','AMAPÁ','AMAZONAS','BAHIA','CEARÁ','DISTRITO FEDERAL','ESPÍRITO SANTO','GOIÁS','MARANHÃO','MATO GROSSO','MATO GROSSO DO SUL','MINAS GERAIS','PARÁ','PARAÍBA','PARANÁ','PERNAMBUCO','PIAUÍ','RIO DE JANEIRO','RIO GRANDE DO NORTE','RIO GRANDE DO SUL','RONDÔNIA','RORAIMA','SANTA CATARINA','SÃO PAULO','SERGIPE','TOCANTINS')}
 raw=raw[raw['UNIDADE DA FEDERAÇÃO'].map(_norm).isin(valid_states)]
 grouped=raw.groupby(['date','product_key'],as_index=False)['value'].sum()
 pivot=grouped.pivot(index='date',columns='product_key',values='value').reset_index()
 rows=[{'date':r['date'],**{p:r.get(p) for p in PRODUCTS}} for r in pivot.to_dict('records')]
 rows=[r for r in rows if r['date']>=START]
 rows=validate_rows(rows,'m3')
 latest=rows[-1]['date'][:7]
 return {'rows':rows,'native_unit':'m3','source_urls':[url,ANP_LANDING], 'vintage':f'ANP fuel-sales CSV retrieved {datetime.now(timezone.utc).date().isoformat()}; data through {latest}.','validation_note':f'ANP state-level CSV aggregated once across 27 federative units for GASOLINA C, QUEROSENE DE AVIAÇÃO and ÓLEO DIESEL. Final native row {latest}: {rows[-1]["gasoline"]}, {rows[-1]["jet_fuel"]}, {rows[-1]["diesel"]} m3.'}

def fetch_mexico(session,today=None):
 raise RuntimeError('PEMEX RVOLVIN official export parser still requires live historical endpoint verification')

def fetch_australia(session,today=None):
 raise RuntimeError('Australian Petroleum Statistics official workbook parser still requires live attachment verification')
