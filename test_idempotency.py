import http.client
import json
import threading
import time
import unittest
from http.server import ThreadingHTTPServer

import app


class GatewayTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), app.IdempotencyGatewayHandler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        app.reset_state_for_tests()

    def post_payment(self, key, amount=100, currency="GHS", api_key=app.API_KEY):
        body = json.dumps({"amount": amount, "currency": currency})
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        headers = {"Content-Type": "application/json", "Idempotency-Key": key}
        if api_key is not None:
            headers["X-API-Key"] = api_key
        connection.request(
            "POST",
            "/process-payment",
            body=body,
            headers=headers,
        )
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
        headers = dict(response.getheaders())
        connection.close()
        return response.status, headers, payload

    def test_first_request_processes_payment(self):
        status, headers, payload = self.post_payment("test-first")

        self.assertEqual(status, 201)
        self.assertEqual(headers["X-Cache-Hit"], "false")
        self.assertEqual(payload["message"], "Charged 100 GHS")
        self.assertEqual(len(app.get_recent_transactions()), 1)

    def test_duplicate_request_replays_saved_response(self):
        first_status, _, first_payload = self.post_payment("test-duplicate")

        started = time.perf_counter()
        second_status, headers, second_payload = self.post_payment("test-duplicate")
        elapsed = time.perf_counter() - started

        self.assertEqual(first_status, second_status)
        self.assertEqual(first_payload, second_payload)
        self.assertEqual(headers["X-Cache-Hit"], "true")
        self.assertLess(elapsed, app.PROCESSING_DELAY_SECONDS)
        self.assertEqual(len(app.get_recent_transactions()), 1)

    def test_same_key_different_body_is_rejected(self):
        self.post_payment("test-conflict", amount=100)
        status, _, payload = self.post_payment("test-conflict", amount=500)

        self.assertEqual(status, 422)
        self.assertEqual(
            payload["error"]["message"],
            "Idempotency key already used for a different request body.",
        )
        self.assertEqual(len(app.get_recent_transactions()), 1)

    def test_in_flight_duplicate_waits_for_first_response(self):
        results = []

        def send():
            results.append(self.post_payment("test-in-flight"))

        thread_a = threading.Thread(target=send)
        thread_b = threading.Thread(target=send)

        started = time.perf_counter()
        thread_a.start()
        time.sleep(0.1)
        thread_b.start()
        thread_a.join()
        thread_b.join()
        elapsed = time.perf_counter() - started

        statuses = [result[0] for result in results]
        payloads = [result[2] for result in results]
        cache_hits = [result[1].get("X-Cache-Hit") for result in results]

        self.assertEqual(statuses, [201, 201])
        self.assertEqual(payloads[0], payloads[1])
        self.assertIn("true", cache_hits)
        self.assertLess(elapsed, app.PROCESSING_DELAY_SECONDS + 1)
        self.assertEqual(len(app.get_recent_transactions()), 1)

    def test_missing_api_key_is_rejected(self):
        status, _, payload = self.post_payment("test-auth", api_key=None)

        self.assertEqual(status, 401)
        self.assertEqual(payload["error"]["code"], "UNAUTHORIZED")

    def test_payment_status_endpoint_returns_transaction(self):
        _, _, payload = self.post_payment("test-status")
        transaction_id = payload["transaction"]["transaction_id"]

        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        connection.request(
            "GET",
            f"/payments/{transaction_id}",
            headers={"X-API-Key": app.API_KEY},
        )
        response = connection.getresponse()
        status_payload = json.loads(response.read().decode("utf-8"))
        connection.close()

        self.assertEqual(response.status, 200)
        self.assertEqual(status_payload["transaction"]["transaction_id"], transaction_id)


if __name__ == "__main__":
    unittest.main()
