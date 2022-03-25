#!/usr/bin/env python3

'''Scrape the data underlying a Kubra electrical outages map and collect
it in a SQLite database.'''

import os, re, random, time, json, sqlite3
from backports.zoneinfo import ZoneInfo
from datetime import datetime, date, time as Time, timedelta

import requests, pyquadkey2.quadkey
from pypolyline.cutil import decode_polyline

# ------------------------------------------------------------
# * Parameters
# ------------------------------------------------------------

with open('config.json') as config:
    config = json.load(config)

sleep_ranges_seconds = dict(
    between_requests = (.5, 2),
    retry = (15*60, 16*60))
request_sleep_interval = 30
max_tries = 3

# We decided on these numbers based on exhaustive checks of data
# availability with the first few days of July 2020 and 2021 in tile
# 03201011.
query_time_increment = timedelta(minutes = 15)
subquery_time_increment = timedelta(seconds = 30)
subquery_time_max_dist = timedelta(minutes = 1, seconds = 30)

sites = [{'code': i, **d} for i, d in enumerate([
    # All `date_min` values are my best guess from testing.
    dict(
        name = 'nyc',
        url_root = 'https://outagemap.coned.com/resources/data/external/interval_generation_data',
        tz = ZoneInfo('America/New_York'),
        top_tiles = (pyquadkey2.quadkey.from_str('03201011'),),
        date_min = date(2020, 2, 19))])]

na_like_values = ('Not Supplied',)

# ------------------------------------------------------------
# * Helpers
# ------------------------------------------------------------

def el1(x):
    'Ensure the given iterable has only one value, and return it.'
    x, = x
    return x

def val1(x, k):
    'Ensure the given dictionary has only one value, and return it.'
    assert tuple(x.keys()) == (k,)
    return x[k]

def point_in_tile(p, tile):
    '''Test whether the given (lon, lat) pair is in the tile indicated by
    the given quadkey.'''
    # `pypolyline` produces coordinates in lon-lat order whereas
    # `pyquadkey2` uses lat-lon order.
    lon, lat = p
    lat_min, lon_min = tile.to_geo(pyquadkey2.quadkey.TileAnchor.ANCHOR_SW)
    lat_max, lon_max = tile.to_geo(pyquadkey2.quadkey.TileAnchor.ANCHOR_NE)
    return lon_min <= lon <= lon_max and lat_min <= lat <= lat_max

def msg(*args, **kwargs):
    print(time.strftime('%Y-%m-%d %H:%M:%S - '), end = '')
    print(*args, **kwargs)

def sleep(k):
    msg('Sleeping', end = '', flush = True)
    time.sleep(random.uniform(*sleep_ranges_seconds[k]))
    print(' - done')

n_requests = 0
def sleepy_get(*args, **kwargs):
    r = requests.get(*args, **kwargs)
    global n_requests
    n_requests += 1
    if n_requests % request_sleep_interval == 0:
        sleep('between_requests')
    return r

# ------------------------------------------------------------
# * Database setup
# ------------------------------------------------------------

db = None
enums = {}

def init_db():
    global db, enums

    create = not os.path.exists(config['db.path'])
    db = sqlite3.connect(config['db.path'], isolation_level = None)
    db.execute('pragma foreign_keys = true')

    if create:
        db.execute('pragma journal_mode = wal')
        db.execute('''create table Jobs
           (job_id       integer primary key,
            site         integer not null,
            time_max     integer not null,
            time_next    integer)
              -- The next timepoint we need to fetch.
              -- If null, start from the beginning of the data.''')
        db.execute(
            '''create table Events
               (site      integer not null,
                ilon      integer not null,
                ilat      integer not null,
                time      integer not null,
                outage_ix integer not null,
                etr       integer,
                cust_a    integer not null,
                {},
                primary key (site, ilon, ilat, time, outage_ix))'''.format(
            ',\n'.join(
                f'{ec} integer references Enumeration_{ec}(code)'
                for ec in config['enum.cols'])))
        for ec in config['enum.cols']:
            db.execute(f'''create table Enumeration_{ec}
               (code integer primary key,
                meaning text unique not null)''')
        msg('A new database was created.')
        msg('Add one or more jobs with `sqlite3`, then run this program again to start scraping.')
        exit()

    enums = {
        ec: {
            meaning: code
            for code, meaning in db.execute(f'select * from Enumeration_{ec}')}
        for ec in config['enum.cols']}

# ------------------------------------------------------------
# * Scraping logic
# ------------------------------------------------------------

def scrape(site, the_time):

    events = []
    tiles = list(site['top_tiles'])
    time_initial = the_time

    while tiles:
        tile = tiles.pop(0)

        tries = 0
        while True:
            r = sleepy_get('{}/{}/outages/{}.json'.format(
                site['url_root'],
                the_time.strftime('%Y_%m_%d_%H_%M_%S'),
                tile))
            tries += 1
            if r.ok:
                break
            elif r.status_code == requests.codes.forbidden:
                if tile == site['top_tiles'][0]:
                    # Data for this hour may be available at a later
                    # timestamp.
                    the_time += subquery_time_increment
                      # `datetime` doesn't support leap seconds, so this
                      # should always keep us aligned on 30-s intervals.
                    if the_time - time_initial > subquery_time_max_dist:
                        # Give up.
                        break
                    tries = 0
                    continue
                else:
                    # This tile seems to be just plain missing, and there's
                    # nothing we can do.
                    break
            elif tries >= max_tries:
                 raise ValueError('Retries exceeded:', r.url, r.status_code, r.reason)
            sleep('retry')
        if not r.ok:
            continue

        for outage in r.json()['file_data']:
            p = outage['geom']
            p.pop('a', None)
              # Perhaps this element indicates the area of faulty wiring,
              # or the area cordoned off for repairs.
            p = el1(decode_polyline(
                el1(val1(p, 'p')).encode('ASCII'),
                config['polyline.precision']))

            if not outage['desc']['cluster']:
                # This `outage` represents a single event.
                assert sorted(outage.keys()) == ['desc', 'geom', 'id', 'title']
                assert outage['title'] == 'Outage Information'
                assert outage['desc']['outages'] is None
                events.append((p, 1, outage['desc']))
                continue
            # Otherwise, `outage` represents a cluster of events.
            assert outage['title'] == 'Area Outage'

            if sub := outage['desc']['outages']:
                # The individual events are stored in `sub`.
                assert val1(outage['desc']['cust_a'], 'val') == sum(
                    val1(s['cust_a'], 'val') for s in sub)
                events.extend((p, i + 1, s) for i, s in enumerate(sub))
            else:
                # We'll need to request zoomed-in tiles to get the event
                # information.
                for child in tile.children():
                    if point_in_tile(p, child) and child not in tiles:
                        tiles.append(child)

    return events, the_time

def save(site, events, the_time):
    if not events:
        return

    out = []
    global enums

    # Loop through `events` to update `enums` and make rows for `out`.
    for (lon, lat), outage_ix, outage_desc in events:
        assert sorted(outage_desc.keys()) == ['cause', 'cluster', 'crew_status', 'cust_a', 'etr', 'outages', 'reported_problem']

        for ec in config['enum.cols']:
            if outage_desc[ec] in na_like_values:
                outage_desc[ec] = None
            if outage_desc[ec]:
                enums[ec].setdefault(outage_desc[ec], len(enums[ec]))

        d = dict(
            site = site['code'],
            ilon = round(lon * 10**config['polyline.precision']),
            ilat = round(lat * 10**config['polyline.precision']),
            time = int(the_time.timestamp()),
            outage_ix = outage_ix,
            cust_a = val1(outage_desc['cust_a'], 'val'),
            etr = (
               None if outage_desc['etr'] == 'ETR-NULL' else
               -1 if outage_desc['etr'] == 'ETR-EXP' else
               int(
                   datetime.fromisoformat(
                       re.sub(r'([+-]\d\d)(\d\d)\Z', r'\1:\2',
                           outage_desc['etr']))
                   .timestamp())),
            **{ec: outage_desc[ec] and enums[ec][outage_desc[ec]]
                for ec in config['enum.cols']})
        if d not in out:
            out.append(d)

    # Update the database. (This function doesn't itself initiate a
    # transaction, but it's called inside a transaction.)
    for ec in config['enum.cols']:
        db.executemany(
            f'insert or ignore into Enumeration_{ec} values (?, ?)',
            ((code, meaning) for (meaning, code) in enums[ec].items()))
    db.executemany(
        'insert into Events ({}) values ({})'.format(
            ', '.join(sorted(out[0].keys())),
            ', '.join(':' + k for k in sorted(out[0].keys()))),
        out)

# ------------------------------------------------------------
# * Mainline code
# ------------------------------------------------------------

def main():
    'Complete each job in the `Jobs` table.'

    while True:
        # Loop over jobs and hours.

        job = list(db.execute('''select
            job_id, site, time_next
            from Jobs
            where time_next is null or time_next <= time_max
            order by job_id limit 1'''))
        if not job:
            msg('All jobs done.')
            return
        (job_id, site, time_next), = job
        site = sites[site]
        time_next = (datetime.fromtimestamp(time_next, site['tz'])
            if time_next
            else datetime.combine(site['date_min'], Time(), site['tz']))

        msg(f'Scraping job {job_id}, site {site["name"]}, time {time_next}')
        events, time_actual = scrape(site, time_next)

        msg('Writing')
        db.execute('begin')
        save(site, events, time_actual)
        time_next += query_time_increment
        db.execute(
            'update Jobs set time_next = ? where job_id = ?',
            (int(time_next.timestamp()), job_id))
        db.execute('commit')

if __name__ == '__main__':
    try:
        init_db()
        main()
    finally:
        if db is not None:
            db.close()
