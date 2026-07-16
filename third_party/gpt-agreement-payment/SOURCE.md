# ChatGPT Authentication Reference

The password and TOTP challenge flow was validated against:

- `RoodraNambisa/Gpt-Agreement-Payment` at commit `7a004ff3879f53b0461401b4b3fcd727676ff5b0`.

The Go implementation follows the observed `mfa_challenge` selection, RFC 6238 code generation, `mfa_request_id` propagation, and `/api/accounts/mfa/verify` request shape. The accompanying `LICENSE` is retained for the adapted authentication behavior.
