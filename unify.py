# -*- coding: utf-8 -*-
# Generic key-normalization worker. All table/column names + credentials come from env (CI secrets).
# Derives a canonical id from a reference field and rewrites rows + dependent columns. Idempotent
# (skips rows already canonical); excludes ambiguous collisions; never deletes.
import os, re, sys, requests, urllib3, threading
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict, Counter
from requests.adapters import HTTPAdapter
urllib3.disable_warnings()

T = os.environ['API_TOKEN']; A = os.environ['ACCOUNT_ID']; D = os.environ['DB_ID']
TBL = os.environ['MAIN_TABLE']; IDC = os.environ['ID_COL']; RQ = os.environ['REQ_COL']; PFX = os.environ['PFX_COL']
MIG = [x.split(':') for x in os.environ.get('MIGRATE', '').split(',') if ':' in x]
WORKERS = int(os.environ.get('WORKERS', '10'))
MODE = os.environ.get('MODE', 'dry')   # dry(只算不写)/ apply

S = requests.Session(); S.trust_env = False
S.mount('https://', HTTPAdapter(pool_connections=24, pool_maxsize=24, max_retries=3))
URL = f'https://api.cloudflare.com/client/v4/accounts/{A}/d1/database/{D}/query'

def q(sql):
    r = S.post(URL, json={'sql': sql}, headers={'Authorization': f'Bearer {T}'}, verify=False, timeout=90).json()
    if not r.get('success'):
        raise SystemExit(f'[FATAL] {str(r.get("errors"))[:200]}')
    return r['result'][0].get('results', [])

def qx(sql):
    try:
        r = S.post(URL, json={'sql': sql}, headers={'Authorization': f'Bearer {T}'}, verify=False, timeout=90).json()
    except Exception as e:
        return False, str(e)[:120]
    return bool(r.get('success')), str(r.get('errors'))[:120]

def build():
    rows = q(f'SELECT {IDC}, {RQ}, {PFX} FROM {TBL}')
    learn = defaultdict(Counter)
    for r in rows:
        mb = re.match(r'^([a-z]+)(\d.*)$', r[IDC] or ''); mr = re.match(r'^([一-鿿]+)(\d.*)$', r.get(RQ) or '')
        if mb and mr:
            learn[mr.group(1)][mb.group(1)] += 1
    pmap = {k: c.most_common(1)[0][0] for k, c in learn.items()}

    def tgt(r):
        bid = r[IDC] or ''; rq = (r.get(RQ) or '').strip()
        if not rq or '-' not in bid:
            return bid
        mr = re.match(r'^([一-鿿]*)(\d.*)$', rq)
        if not mr:
            return bid
        vol = bid.rsplit('-', 1)[1]
        if not re.match(r'^\d+$', vol):
            return bid
        return f"{pmap.get(mr.group(1), '') if mr.group(1) else ''}{mr.group(2)}-{vol}"

    tmap = defaultdict(list)
    for r in rows:
        r['_t'] = tgt(r); tmap[r['_t']].append(r)
    bad = set()
    for t, rs in tmap.items():
        if len(rs) > 1:
            for x in rs:
                bad.add(x[IDC])
    def has_part(rq):
        return bool(re.match(r'^[一-鿿]', (rq or '').strip()))
    for r in rows:
        if (r.get(RQ) or '').strip() and not has_part(r.get(RQ)) and r['_t'] != r[IDC]:
            bad.add(r[IDC])
    return [(r[IDC], r['_t']) for r in rows if r['_t'] != r[IDC] and r[IDC] not in bad]

def main():
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    changes = build()
    cols = [(t, c) for t, c in MIG if qx(f'SELECT {c} FROM {t} LIMIT 0')[0]]
    print(f'[plan] changes={len(changes)} migrate_tables={[t for t,_ in cols]} mode={MODE}', flush=True)
    print(f'[plan] sample={changes[:5]}', flush=True)
    if MODE != 'apply':
        print('[dry-run] computed only · NO write. 核对 changes 数与碰撞排除后,再以 MODE=apply 跑。', flush=True)
        return
    n = len(changes); lock = threading.Lock(); st = {'ok': 0, 'err': 0}

    def apply(pair):
        old, new = pair
        stmts = [f"UPDATE {TBL} SET {IDC}='{new}', updated_at=strftime('%s','now') WHERE {IDC}='{old}'"]
        for t, c in cols:
            stmts.append(f"UPDATE {t} SET {c}='{new}' WHERE {c}='{old}'")
        ok, err = qx('; '.join(stmts))
        with lock:
            if ok: st['ok'] += 1
            else: st['err'] += 1; print(f'  [ERR] {old}->{new}: {err}', flush=True)
            d = st['ok'] + st['err']
            if d % 1000 == 0 or d == n:
                print(f'  [{d}/{n}] ok={st["ok"]} err={st["err"]}', flush=True)

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(apply, changes))
    print(f'[done] ok={st["ok"]} err={st["err"]}', flush=True)
    sys.exit(1 if st['err'] else 0)

if __name__ == '__main__':
    main()
