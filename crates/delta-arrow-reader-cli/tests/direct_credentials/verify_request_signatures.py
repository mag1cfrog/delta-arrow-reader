"""Verify captured read requests with fixture keys, independently of object_store.

Protocol references:
https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html
https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key
"""

import base64
import hashlib
import hmac
import json
import sys
from urllib.parse import parse_qsl, quote, urlsplit


def verify_s3_signature(method, target, headers, signing_keys):
    scheme, authorization = headers["authorization"].split(" ", 1)
    assert scheme == "AWS4-HMAC-SHA256"
    fields = dict(field.split("=", 1) for field in authorization.split(", "))
    access_key, scope = fields["Credential"].split("/", 1)
    date, region, service, terminator = scope.split("/")
    assert service == "s3" and terminator == "aws4_request"
    signed_headers = fields["SignedHeaders"]
    canonical_headers = "".join(
        f"{name}:{headers[name]}\n" for name in signed_headers.split(";")
    )
    url = urlsplit(target)
    query_pairs = sorted(
        (quote(name, safe="-_.~"), quote(value, safe="-_.~"))
        for name, value in parse_qsl(url.query, keep_blank_values=True)
    )
    canonical_query = "&".join(f"{name}={value}" for name, value in query_pairs)
    # S3 signs the encoded path as sent, without normalizing or double-encoding it.
    canonical_request = "\n".join([
        method, url.path, canonical_query, canonical_headers,
        signed_headers, headers["x-amz-content-sha256"],
    ])
    string_to_sign = "\n".join([
        scheme, headers["x-amz-date"], scope,
        hashlib.sha256(canonical_request.encode()).hexdigest(),
    ])
    signing_key = ("AWS4" + signing_keys[access_key]).encode()
    for component in [date, region, service, terminator]:
        signing_key = hmac.digest(signing_key, component.encode(), "sha256")
    expected = hmac.new(signing_key, string_to_sign.encode(), "sha256").hexdigest()
    assert hmac.compare_digest(expected, fields["Signature"]), "invalid S3 signature"


def verify_azure_signature(method, target, headers, signing_keys):
    scheme, authorization = headers["authorization"].split(" ", 1)
    assert scheme == "SharedKey"
    account, signature = authorization.split(":", 1)
    standard_headers = [
        "content-encoding", "content-language", "content-length", "content-md5",
        "content-type", "date", "if-modified-since", "if-match", "if-none-match",
        "if-unmodified-since", "range",
    ]
    values = {name: headers.get(name, "") for name in standard_headers}
    # Modern Azure APIs sign zero Content-Length and Date with x-ms-date as empty.
    if values["content-length"] == "0":
        values["content-length"] = ""
    if "x-ms-date" in headers:
        values["date"] = ""
    canonical_headers = "".join(
        f"{name}:{value}\n"
        for name, value in sorted(headers.items()) if name.startswith("x-ms-")
    )
    url = urlsplit(target)
    query_values = {}
    for name, value in parse_qsl(url.query, keep_blank_values=True):
        query_values.setdefault(name.lower(), []).append(value)
    canonical_resource = f"/{account}{url.path}" + "".join(
        f"\n{name}:{','.join(sorted(values))}"
        for name, values in sorted(query_values.items())
    )
    string_to_sign = "\n".join([method] + [values[name] for name in standard_headers])
    string_to_sign += "\n" + canonical_headers + canonical_resource
    account_key = base64.b64decode(signing_keys[account], validate=True)
    expected = base64.b64encode(hmac.digest(account_key, string_to_sign.encode(), "sha256"))
    assert hmac.compare_digest(expected.decode(), signature), "invalid Azure signature"


def main():
    with open(sys.argv[1]) as fixture_file:
        fixture = json.load(fixture_file)
    verifier = {"s3": verify_s3_signature, "azure": verify_azure_signature}[fixture["protocol"]]
    assert fixture["requests"], "no requests to verify"
    for request in fixture["requests"]:
        header_block, body = request.split("\r\n\r\n", 1)
        request_line, *header_lines = header_block.split("\r\n")
        method, target, _ = request_line.split(" ")
        assert method in ("GET", "HEAD") and not body, "expected a bodyless read request"
        headers = {}
        for line in header_lines:
            name, value = line.split(":", 1)
            headers[name.lower()] = " ".join(value.split())
        verifier(method, target, headers, fixture["signing_keys"])


if __name__ == "__main__":
    main()
