import unittest

from starlette.requests import Request
from starlette.responses import Response

from app.rate_limit import (
    SlidingWindowRateLimiter,
    apply_rate_limit_headers,
    client_ip,
)


class RateLimitTest(unittest.TestCase):
    def test_sliding_window_blocks_and_expires_without_extending_the_block(self):
        now = [0.0]
        limiter = SlidingWindowRateLimiter(2, 60, clock=lambda: now[0])

        first = limiter.check("client", consume=True)
        second = limiter.check("client", consume=True)
        blocked = limiter.check("client", consume=True)

        self.assertTrue(first.allowed)
        self.assertEqual(first.remaining, 1)
        self.assertTrue(second.allowed)
        self.assertEqual(second.remaining, 0)
        self.assertFalse(blocked.allowed)
        self.assertEqual(blocked.reset_after, 60)

        now[0] = 59
        self.assertEqual(limiter.check("client").reset_after, 1)
        now[0] = 60
        expired = limiter.check("client")
        self.assertTrue(expired.allowed)
        self.assertEqual(expired.remaining, 2)

    def test_headers_include_quota_reset_scope_and_retry_after(self):
        limiter = SlidingWindowRateLimiter(1, 30, clock=lambda: 0.0)
        limiter.check("client", consume=True)
        decision = limiter.check("client")
        response = apply_rate_limit_headers(
            Response(status_code=429), decision, scope="test"
        )

        self.assertEqual(response.headers["x-ratelimit-limit"], "1")
        self.assertEqual(response.headers["x-ratelimit-remaining"], "0")
        self.assertEqual(response.headers["x-ratelimit-window"], "30")
        self.assertEqual(response.headers["x-ratelimit-scope"], "test")
        self.assertEqual(response.headers["retry-after"], "30")
        self.assertGreater(int(response.headers["x-ratelimit-reset"]), 0)

    def test_key_capacity_fails_closed_instead_of_growing_memory(self):
        limiter = SlidingWindowRateLimiter(2, 60, clock=lambda: 0.0, max_keys=1)
        self.assertTrue(limiter.check("first", consume=True).allowed)
        self.assertFalse(limiter.check("second", consume=True).allowed)
        self.assertTrue(limiter.check("first", consume=True).allowed)

    def test_client_ip_uses_asgi_address_and_ignores_raw_forwarded_headers(self):
        request = Request(
            {
                "type": "http",
                "method": "GET",
                "scheme": "https",
                "path": "/",
                "query_string": b"",
                "headers": [(b"x-forwarded-for", b"203.0.113.10")],
                "client": ("192.0.2.7", 12345),
                "server": ("testserver", 443),
            }
        )
        self.assertEqual(client_ip(request), "192.0.2.7")


if __name__ == "__main__":
    unittest.main()
