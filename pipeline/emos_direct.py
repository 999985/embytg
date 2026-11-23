#!/usr/bin/env python3
import json
import urllib.request

UA = 'Hills Windows/1.5.1 (windows; 19041.vb_release.191206-1406)'


def direct_url(host, token, item_id, timeout=30):
    host = (host or '').rstrip('/')
    url = '%s/Items/%s/PlaybackInfo?UserId=Me&api_key=%s' % (host, item_id, token)
    req = urllib.request.Request(
        url, data=b'{}', method='POST',
        headers={'User-Agent': UA, 'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        d = json.loads(r.read() or b'{}')
    ms = (d.get('MediaSources') or [{}])[0]
    p = ms.get('DirectStreamUrl') or ''
    if not p:
        return ''
    return p if p.startswith('http') else host + p
