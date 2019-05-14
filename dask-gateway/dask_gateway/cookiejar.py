# This code was borrowed and modified from aiohttp to work with tornado. The
# original source can be found here:
#
# https://github.com/aio-libs/aiohttp/blob/master/aiohttp/cookiejar.py
#
# The original aiohttp code is licensed as follows:
#
# Copyright 2013-2019 Nikolay Kim and Andrew Svetlov
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
import datetime
import re
from collections import defaultdict
from http.cookies import SimpleCookie
from math import ceil
from urllib.parse import urlparse

from tornado.gen import IOLoop


_DATE_TOKENS_RE = re.compile(
    r"[\x09\x20-\x2F\x3B-\x40\x5B-\x60\x7B-\x7E]*"
    r"(?P<token>[\x00-\x08\x0A-\x1F\d:a-zA-Z\x7F-\xFF]+)"
)

_DATE_HMS_TIME_RE = re.compile(r"(\d{1,2}):(\d{1,2}):(\d{1,2})")

_DATE_DAY_OF_MONTH_RE = re.compile(r"(\d{1,2})")

_DATE_MONTH_RE = re.compile(
    "(jan)|(feb)|(mar)|(apr)|(may)|(jun)|(jul)|" "(aug)|(sep)|(oct)|(nov)|(dec)", re.I
)

_DATE_YEAR_RE = re.compile(r"(\d{2,4})")


def _is_domain_match(domain, hostname):
    """Implements domain matching adhering to RFC 6265."""
    if hostname == domain:
        return True

    if not hostname.endswith(domain):
        return False

    return hostname[: -len(domain)].endswith(".")


def _is_path_match(req_path, cookie_path):
    """Implements path matching adhering to RFC 6265."""
    if not req_path.startswith("/"):
        req_path = "/"

    if req_path == cookie_path:
        return True

    if not req_path.startswith(cookie_path):
        return False

    if cookie_path.endswith("/"):
        return True

    return req_path[len(cookie_path) :].startswith("/")


def _parse_date(date_str):
    """Implements date string parsing adhering to RFC 6265."""
    if not date_str:
        return None

    found_time = False
    found_day = False
    found_month = False
    found_year = False

    hour = minute = second = 0
    day = 0
    month = 0
    year = 0

    for token_match in _DATE_TOKENS_RE.finditer(date_str):

        token = token_match.group("token")

        if not found_time:
            time_match = _DATE_HMS_TIME_RE.match(token)
            if time_match:
                found_time = True
                hour, minute, second = [int(s) for s in time_match.groups()]
                continue

        if not found_day:
            day_match = _DATE_DAY_OF_MONTH_RE.match(token)
            if day_match:
                found_day = True
                day = int(day_match.group())
                continue

        if not found_month:
            month_match = _DATE_MONTH_RE.match(token)
            if month_match:
                found_month = True
                month = month_match.lastindex
                continue

        if not found_year:
            year_match = _DATE_YEAR_RE.match(token)
            if year_match:
                found_year = True
                year = int(year_match.group())

    if 70 <= year <= 99:
        year += 1900
    elif 0 <= year <= 69:
        year += 2000

    if False in (found_day, found_month, found_year, found_time):
        return None

    if not 1 <= day <= 31:
        return None

    if year < 1601 or hour > 23 or minute > 59 or second > 59:
        return None

    return datetime.datetime(
        year, month, day, hour, minute, second, tzinfo=datetime.timezone.utc
    )


class CookieJar(object):
    """Implements cookie storage adhering to RFC 6265."""

    _MAX_TIME = 2051215261.0  # so far in future (2035-01-01)

    def __init__(self, loop=None):
        self._loop = loop or IOLoop.current()
        self._cookies = defaultdict(SimpleCookie)
        self._host_only_cookies = set()
        self._next_expiration = ceil(self._loop.time())
        self._expirations = {}

    def clear(self):
        self._cookies.clear()
        self._host_only_cookies.clear()
        self._next_expiration = ceil(self._loop.time())
        self._expirations.clear()

    def __iter__(self):
        self._do_expiration()
        return (m for c in self._cookies.values() for m in c.values())

    def __len__(self):
        self._do_expiration()
        return sum(map(len, self._cookies.values()))

    def _do_expiration(self):
        now = self._loop.time()
        if self._next_expiration > now:
            return
        if not self._expirations:
            return
        next_expiration = self._MAX_TIME
        to_del = []
        cookies = self._cookies
        expirations = self._expirations
        for (domain, name), when in expirations.items():
            if when <= now:
                cookies[domain].pop(name, None)
                to_del.append((domain, name))
                self._host_only_cookies.discard((domain, name))
            else:
                next_expiration = min(next_expiration, when)
        for key in to_del:
            del expirations[key]

        self._next_expiration = ceil(next_expiration)

    def _expire_cookie(self, when, domain, name):
        iwhen = int(when)
        self._next_expiration = min(self._next_expiration, iwhen)
        self._expirations[(domain, name)] = iwhen

    def update_cookies(self, cookies, effective_url=None):
        """Update cookies."""
        if effective_url:
            effective_url = urlparse(effective_url)
            hostname = effective_url.hostname
            url_path = effective_url.path
        else:
            hostname = None
            url_path = "/"

        if isinstance(cookies, str):
            cookies = SimpleCookie(cookies)

        for name, cookie in cookies.items():
            domain = cookie["domain"]

            # ignore domains with trailing dots
            if domain.endswith("."):
                domain = ""
                del cookie["domain"]

            if not domain and hostname is not None:
                # Set the cookie's domain to the response hostname
                # and set its host-only-flag
                self._host_only_cookies.add((hostname, name))
                domain = cookie["domain"] = hostname

            if domain.startswith("."):
                # Remove leading dot
                domain = domain[1:]
                cookie["domain"] = domain

            if hostname and not _is_domain_match(domain, hostname):
                # Setting cookies for different domains is not allowed
                continue

            path = cookie["path"]
            if not path or not path.startswith("/"):
                # Set the cookie's path to the response path
                path = url_path
                if not path.startswith("/"):
                    path = "/"
                else:
                    # Cut everything from the last slash to the end
                    path = "/" + path[1 : path.rfind("/")]
                cookie["path"] = path

            max_age = cookie["max-age"]
            if max_age:
                try:
                    delta_seconds = int(max_age)
                    self._expire_cookie(self._loop.time() + delta_seconds, domain, name)
                except ValueError:
                    cookie["max-age"] = ""

            else:
                expires = cookie["expires"]
                if expires:
                    expire_time = _parse_date(expires)
                    if expire_time:
                        self._expire_cookie(expire_time.timestamp(), domain, name)
                    else:
                        cookie["expires"] = ""

            self._cookies[domain][name] = cookie

        self._do_expiration()

    def filter_cookies(self, request_url):
        """Returns this jar's cookies filtered for a request url."""
        self._do_expiration()
        request_url = urlparse(request_url)
        hostname = request_url.hostname or ""
        filtered = SimpleCookie()
        is_not_secure = request_url.scheme not in ("https", "wss")

        for cookie in self._cookies.values():
            for morsel in cookie.values():
                name = morsel.key
                domain = morsel["domain"]

                # Send shared cookies
                if not domain:
                    filtered[name] = morsel
                    continue

                if (domain, name) in self._host_only_cookies:
                    if domain != hostname:
                        continue
                elif not _is_domain_match(domain, hostname):
                    continue

                if not _is_path_match(request_url.path, morsel["path"]):
                    continue

                if is_not_secure and morsel["secure"]:
                    continue

                filtered[name] = morsel

        return filtered

    def pre_request(self, req):
        """Update a HTTPRequest with any relevant cookies"""
        cookies = self.filter_cookies(req.url)
        for v in cookies.values():
            req.headers.add("Cookie", v.OutputString(attrs=()))

    def post_response(self, resp):
        """Update our cookiejar with the results from a HTTPResponse"""
        new = resp.headers.get_list("Set-Cookie")
        if new:
            cookies = SimpleCookie()
            for m in new:
                cookies.load(m)
            self.update_cookies(cookies, resp.effective_url)
