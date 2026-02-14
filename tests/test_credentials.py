from __future__ import annotations

from auth import credentials


class _FakeClient:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.creds = None

    def set_api_creds(self, creds):
        self.creds = creds


def test_create_api_key_uses_post_and_nonce(monkeypatch) -> None:
    called = {}

    class _Signer:
        def __init__(self, private_key: str, chain_id: int) -> None:
            called["signer"] = (private_key, chain_id)

    monkeypatch.setattr(credentials, "Signer", _Signer)
    monkeypatch.setattr(credentials, "create_level_1_headers", lambda _signer, nonce: {"nonce": str(nonce)})

    def _fake_post(endpoint, headers=None):
        called["post"] = (endpoint, headers)
        return {"apiKey": "k", "secret": "s", "passphrase": "p"}

    monkeypatch.setattr(credentials, "post", _fake_post)

    result = credentials.create_api_key("0xabc", 137, nonce=11)

    assert result.api_key == "k"
    assert result.nonce == 11
    assert called["post"][0].endswith("/auth/api-key")


def test_derive_api_key_uses_get_and_nonce(monkeypatch) -> None:
    monkeypatch.setattr(credentials, "create_level_1_headers", lambda _signer, nonce: {"nonce": str(nonce)})
    monkeypatch.setattr(credentials, "Signer", lambda *_args, **_kwargs: object())

    called = {}

    def _fake_get(endpoint, headers=None):
        called["get"] = (endpoint, headers)
        return {"apiKey": "k2", "secret": "s2", "passphrase": "p2"}

    monkeypatch.setattr(credentials, "get", _fake_get)

    result = credentials.derive_api_key("0xabc", 137, nonce=19)

    assert result.api_key == "k2"
    assert result.nonce == 19
    assert called["get"][0].endswith("/auth/derive-api-key")


def test_init_client_requires_signature_and_funder() -> None:
    try:
        credentials.init_client(
            signer="0xabc",
            derived_creds=None,
            signature_type=None,
            funder_address="0xfunder",
            host="https://clob.polymarket.com",
            chain_id=137,
            clob_client_cls=_FakeClient,
        )
        assert False, "expected validation error"
    except credentials.CredentialValidationError:
        pass

    try:
        credentials.init_client(
            signer="0xabc",
            derived_creds=None,
            signature_type=2,
            funder_address="",
            host="https://clob.polymarket.com",
            chain_id=137,
            clob_client_cls=_FakeClient,
        )
        assert False, "expected validation error"
    except credentials.CredentialValidationError:
        pass


def test_rotation_renews_expired_creds(monkeypatch) -> None:
    stale = credentials.ManagedApiCreds(
        api_key="a",
        api_secret="b",
        api_passphrase="c",
        nonce=1,
        created_at=0,
        last_derived_at=0,
    )
    monkeypatch.setattr(credentials, "derive_api_key", lambda *_args, **_kwargs: credentials.ManagedApiCreds("n", "s", "p", 2, 100, 100))

    rotated = credentials.rotate_api_credentials(
        "0xabc",
        137,
        current_creds=stale,
        rotation_seconds=1,
        nonce=2,
    )

    assert rotated.api_key == "n"
